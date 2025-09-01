import asyncio
import aiohttp
import hashlib
import hmac
import base64
import time
import urllib.parse
import json
import logging
import decimal
from typing import Dict, List, Optional, Any, Union, Callable, Awaitable
from pathlib import Path
import yaml
import websockets

logger = logging.getLogger(__name__)

class KrakenAccountError(Exception):
    """Custom exception for KrakenAccount errors"""
    pass

async def cleanup_all_tasks(exclude_current: bool = True, timeout: float = 5.0) -> None:
    """
    Gets all asyncio tasks and closes them gracefully.
    
    Args:
        exclude_current: If True, excludes the current task from cleanup
        timeout: Maximum time to wait for tasks to complete
    """
    try:
        # Get current task if we need to exclude it
        current_task = None
        if exclude_current:
            try:
                current_task = asyncio.current_task()
            except RuntimeError:
                # No event loop running
                current_task = None
        
        # Get all tasks
        all_tasks = asyncio.all_tasks()
        
        # Filter out current task if requested
        if current_task and exclude_current:
            tasks_to_cancel = [task for task in all_tasks if task != current_task]
        else:
            tasks_to_cancel = list(all_tasks)
        
        if not tasks_to_cancel:
            logger.info("No tasks to cleanup")
            return
        
        logger.info(f"Cleaning up {len(tasks_to_cancel)} asyncio tasks")
        
        # Cancel all tasks
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
                logger.debug(f"Cancelled task: {task.get_name()}")
        
        # Wait for tasks to complete with timeout
        if tasks_to_cancel:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                    timeout=timeout
                )
                logger.info("All tasks cleaned up successfully")
            except asyncio.TimeoutError:
                logger.warning(f"Some tasks did not complete within {timeout}s timeout")
            except Exception as e:
                logger.error(f"Error during task cleanup: {e}")
    
    except Exception as e:
        logger.error(f"Error in cleanup_all_tasks: {e}")

class KrakenAccount:
    """
    Manages trading operations for a Kraken account using WebSocket v2 API.
    This client is designed for real-time order management and private data streams.
    """
    
    API_URL = "https://api.kraken.com"
    WS_URL_V2 = "wss://ws-auth.kraken.com/v2"
    API_VERSION = "0"
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """Initializes the KrakenAccount, loading credentials but not connecting."""
        self.api_key = api_key
        self.api_secret = api_secret
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws_connection: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_authenticated = False
        self._auth_token: Optional[str] = None
        
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._message_handler_task: Optional[asyncio.Task] = None
        self._connection_lock = asyncio.Lock()
        self._cancel_timer_task: Optional[asyncio.Task] = None
        
        # Add handlers for private data streams
        self._private_handlers: Dict[str, List[Callable[[Dict], Awaitable[None]]]] = {}
        self._subscriptions: Dict[str, Dict] = {}
        
        # Cancel on disconnect settings
        self._cancel_on_disconnect_enabled = False
        self._cancel_on_disconnect_timeout: Optional[int] = None

    @classmethod
    async def create(cls, api_key: Optional[str] = None, api_secret: Optional[str] = None) -> 'KrakenAccount':
        """Factory method to create and connect a KrakenAccount instance using v2."""
        account = cls(api_key, api_secret)
        await account.connect_v2()
        return account

    
    def _load_config(self, config_path: Path) -> Dict:
        """Loads configuration from a specified YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.warning(f"Configuration file not found at {config_path}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config: {e}")
            return {}
    
    def _get_rest_signature(self, endpoint: str, data: Dict) -> str:
        """Generates the signature for REST API requests."""
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = endpoint.encode() + hashlib.sha256(encoded).digest()
        
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()
            
    def _get_ws_auth_token_signature(self, data: Dict) -> str:
        """Generates the signature for the WebSocket authentication token."""
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = b'/0/private/GetWebSocketsToken' + hashlib.sha256(encoded).digest()
        
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    async def _get_ws_auth_token(self) -> str:
        """Retrieves a WebSocket authentication token from the Kraken REST API."""
        if not self._session:
            self._session = aiohttp.ClientSession()

        data = {'nonce': str(int(1000 * time.time()))}
        headers = {
            'API-Key': self.api_key,
            'API-Sign': self._get_ws_auth_token_signature(data)
        }
        
        url = f"{self.API_URL}/{self.API_VERSION}/private/GetWebSocketsToken"
        
        async with self._session.post(url, headers=headers, data=data) as response:
            result = await response.json()
            
        if result.get('error'):
            raise KrakenAccountError(f"Failed to get WebSocket token: {result['error']}")
            
        return result['result']['token']

    async def _make_rest_request(self, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Makes a REST API request to Kraken."""
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        if data is None:
            data = {}
        
        data['nonce'] = str(int(1000 * time.time()))
        
        headers = {
            'API-Key': self.api_key,
            'API-Sign': self._get_rest_signature(endpoint, data)
        }
        
        url = f"{self.API_URL}{endpoint}"
        
        async with self._session.post(url, headers=headers, data=data) as response:
            result = await response.json()
            
        if result.get('error'):
            raise KrakenAccountError(f"Kraken API Error: {result['error']}")
            
        return result

    async def connect_v2(self) -> None:
        """Establishes and authenticates the WebSocket v2 connection."""
        async with self._connection_lock:
            if self._ws_connection:# and not self._ws_connection.closed:
                logger.info("WebSocket v2 connection already established.")
                return

            try:
                # Close any existing connection
                if self._ws_connection:# and not self._ws_connection.closed:
                    await self._ws_connection.close()
                
                # Get new auth token
                self._auth_token = await self._get_ws_auth_token()
                
                # Establish new connection
                self._ws_connection = await websockets.connect(
                    self.WS_URL_V2,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=1
                )
                
                self._ws_authenticated = True
                
                # Cancel any existing message handler
                if self._message_handler_task and not self._message_handler_task.done():
                    self._message_handler_task.cancel()
                    try:
                        await self._message_handler_task
                    except asyncio.CancelledError:
                        pass
                    
                # Start new message handler
                self._message_handler_task = asyncio.create_task(self._handle_ws_messages_v2())
                logger.info("WebSocket v2 connection established and authenticated.")

                # Restore subscriptions if any
                if self._subscriptions:
                    logger.info("Restoring active subscriptions...")
                    for sub in self._subscriptions.values():
                        await self._send_subscription_v2(sub)

            except Exception as e:
                logger.error(f"Failed to establish WebSocket v2 connection: {e}")
                self._ws_authenticated = False
                if self._ws_connection:# and not self._ws_connection.closed:
                    await self._ws_connection.close()
                raise KrakenAccountError(f"Failed to connect: {e}")

    # Legacy method for backward compatibility
    async def connect(self) -> None:
        """Legacy connect method - redirects to v2."""
        await self.connect_v2()

    def add_handler(self, event_type: str, handler: Callable[[Dict], Awaitable[None]]) -> None:
        """Add a message handler for a specific private data type (e.g., 'executions')."""
        if event_type not in self._private_handlers:
            self._private_handlers[event_type] = []
        self._private_handlers[event_type].append(handler)
        logger.info(f"Added handler for private {event_type}")

    async def _handle_ws_messages_v2(self) -> None:
        """Continuously listens for and processes incoming WebSocket v2 messages."""
        try:
            async for message in self._ws_connection:  # type: ignore
                try:
                    data = json.loads(message)
                    logger.debug(f"WS v2 Recv: {data}")

                    # Handle method responses (trading requests)
                    if isinstance(data, dict) and "req_id" in data:
                        req_id = data.get("req_id")
                        if req_id in self._pending_requests:
                            future = self._pending_requests.pop(req_id)
                            if not data.get("success", False):
                                error_msg = data.get("error", "Unknown error")
                                future.set_exception(KrakenAccountError(f"Kraken API Error: {error_msg}"))
                            else:
                                future.set_result(data)
                    
                    # Handle private data streams
                    elif isinstance(data, dict) and "channel" in data and "data" in data:
                        channel_name = data["channel"]
                        if channel_name in self._private_handlers:
                            for handler in self._private_handlers[channel_name]:
                                try:
                                    asyncio.create_task(handler(data))
                                except Exception as e:
                                    logger.error(f"Error in '{channel_name}' handler: {e}")
                    
                    # Handle subscription confirmations
                    elif isinstance(data, dict) and data.get("method") == "subscribe":
                        if data.get("success"):
                            logger.info(f"Successfully subscribed to private channel: {data}")
                        else:
                            logger.error(f"Private subscription failed: {data.get('error', 'Unknown error')}")
                    
                    # Handle unsubscription confirmations
                    elif isinstance(data, dict) and data.get("method") == "unsubscribe":
                        if data.get("success"):
                            logger.info(f"Successfully unsubscribed from private channel: {data}")
                        else:
                            logger.error(f"Private unsubscription failed: {data.get('error', 'Unknown error')}")
                    
                    # Handle ping messages
                    elif isinstance(data, dict) and data.get("method") == "ping":
                        pong_message = {"method": "pong", "req_id": data.get("req_id")}
                        try:
                            await self._ws_connection.send(json.dumps(pong_message))  # type: ignore
                            logger.debug("Sent pong response")
                        except websockets.exceptions.ConnectionClosed as e:
                            raise KrakenAccountError(f"Failed to send pong - connection closed: {e}")

                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode WebSocket message: {message}")
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
        
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"WebSocket v2 connection closed: {e}")
            # Trigger task cleanup on disconnect
            try:
                logger.info("Starting task cleanup due to WebSocket disconnection")
                await cleanup_all_tasks(exclude_current=True, timeout=3.0)
            except Exception as cleanup_error:
                logger.error(f"Error during task cleanup: {cleanup_error}")
            
            # NO RECONNECTION - Just raise an error
            raise KrakenAccountError(f"WebSocket connection lost: {e}")
                
        except Exception as e:
            logger.error(f"Error in WebSocket v2 message handler: {e}", exc_info=True)
            # Cleanup tasks on any critical error
            try:
                logger.info("Starting task cleanup due to WebSocket error")
                await cleanup_all_tasks(exclude_current=True, timeout=3.0)
            except Exception as cleanup_error:
                logger.error(f"Error during task cleanup: {cleanup_error}")
            
            raise KrakenAccountError(f"WebSocket message handler error: {e}")
        finally:
            self._ws_authenticated = False
            # Clean up any pending requests that will never resolve
            for future in self._pending_requests.values():
                if not future.done():
                    future.set_exception(KrakenAccountError("WebSocket connection lost."))
            self._pending_requests.clear()

    def connected(self) -> bool:
        """Check if the WebSocket connection is active and authenticated."""
        if not self._ws_authenticated or not self._ws_connection:# or self._ws_connection.closed:
            return False
        return True

    async def _send_request_v2(self, payload: Dict, timeout: float = 10.0) -> Dict:
        """Sends a request over WebSocket v2 and waits for a response."""
        if not self.connected():
            raise KrakenAccountError("WebSocket is not connected. Call connect_v2() first.")

        req_id = int(time.time() * 1000)
        payload["req_id"] = req_id
        
        # Add token to params for v2 API
        if "params" not in payload:
            payload["params"] = {}
        payload["params"]["token"] = self._auth_token

        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future
        
        try:
            await self._ws_connection.send(json.dumps(payload))  # type: ignore
            logger.debug(f"WS v2 Sent: {payload}")
            
            return await asyncio.wait_for(future, timeout=timeout)
        except websockets.exceptions.ConnectionClosed as e:
            if req_id in self._pending_requests:
                del self._pending_requests[req_id]
            raise KrakenAccountError(f"Failed to send request - connection closed: {e}")
        except Exception as e:
            if req_id in self._pending_requests:
                del self._pending_requests[req_id]
            raise KrakenAccountError(f"Failed to send request: {e}")

    async def _send_subscription_v2(self, subscription: Dict) -> None:
        """Sends a subscription message to the private WebSocket v2."""
        if not self.connected():
            raise KrakenAccountError("WebSocket is not connected. Call connect_v2() first.")
        
        # Add req_id and token
        req_id = int(time.time() * 1000)
        subscription["req_id"] = req_id
        
        if "params" not in subscription:
            subscription["params"] = {}
        subscription["params"]["token"] = self._auth_token
        
        try:
            await self._ws_connection.send(json.dumps(subscription))  # type: ignore
            logger.debug(f"WS v2 Subscription Sent: {subscription}")
        except websockets.exceptions.ConnectionClosed as e:
            raise KrakenAccountError(f"Failed to send subscription - connection closed: {e}")
        except Exception as e:
            raise KrakenAccountError(f"Failed to send subscription: {e}")

    # --- Cancel All Orders After (Dead Man's Switch) Methods ---

    async def set_cancel_all_orders_after(self, timeout: int) -> Dict:
        """
        Set up a "Dead Man's Switch" that will cancel all orders after the specified timeout.
        This must be called periodically to reset the timer and prevent cancellation.
        
        Args:
            timeout: Duration in seconds to set/extend the timer (max 86400 seconds).
                    Set to 0 to disable the mechanism.
        
        Returns:
            Response from the server with currentTime and triggerTime.
        """
        if timeout < 0 or timeout > 86400:
            raise ValueError("Timeout must be between 0 and 86400 seconds")
        
        payload = {
            "method": "cancel_all_orders_after",
            "params": {
                "timeout": timeout
            }
        }
        
        try:
            result = await self._send_request_v2(payload)
            
            if timeout > 0:
                self._cancel_on_disconnect_enabled = True
                self._cancel_on_disconnect_timeout = timeout
                logger.info(f"Cancel all orders after timer set to {timeout} seconds")
            else:
                self._cancel_on_disconnect_enabled = False
                self._cancel_on_disconnect_timeout = None
                logger.info("Cancel all orders after timer disabled")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to set cancel all orders after: {e}")
            raise

    async def enable_cancel_on_disconnect(self, timeout: int = 60) -> Dict:
        """
        Enable the "Dead Man's Switch" mechanism with the specified timeout.
        
        Args:
            timeout: Duration in seconds (recommended: 60). Must be > 0.
        
        Returns:
            Response from the server.
        """
        if timeout <= 0:
            raise ValueError("Timeout must be greater than 0 to enable")
        
        return await self.set_cancel_all_orders_after(timeout)

    async def disable_cancel_on_disconnect(self) -> Dict:
        """
        Disable the "Dead Man's Switch" mechanism.
        
        Returns:
            Response from the server.
        """
        return await self.set_cancel_all_orders_after(0)

    async def reset_cancel_timer(self, timeout: int = 60) -> Dict:
        """
        Reset the cancel timer to prevent order cancellation.
        Should be called periodically (every 15-30 seconds recommended).
        
        Args:
            timeout: Duration in seconds to reset the timer to.
        
        Returns:
            Response from the server.
        """
        return await self.set_cancel_all_orders_after(timeout)

    async def get_cancel_on_disconnect_status(self) -> Dict:
        """
        Get the current cancel on disconnect status.
        Note: This returns local state. The actual timer status is on the server.
        
        Returns:
            Dict containing enabled status and timeout (if applicable).
        """
        return {
            "enabled": self._cancel_on_disconnect_enabled,
            "timeout": self._cancel_on_disconnect_timeout,
            "note": "This shows local state. Call set_cancel_all_orders_after() to get server response with current/trigger times."
        }

    async def set_cancel_on_disconnect(self, enabled: bool, timeout: int = 60) -> Dict:
        """
        Convenience method to enable or disable the cancel mechanism.
        
        Args:
            enabled: Whether to enable or disable the mechanism
            timeout: Duration in seconds when enabling (ignored when disabling)
        
        Returns:
            Response from the server.
        """
        if enabled:
            return await self.enable_cancel_on_disconnect(timeout)
        else:
            return await self.disable_cancel_on_disconnect()

    async def start_cancel_timer_task(self, timeout: int = 60, reset_interval: int = 30) -> asyncio.Task:
        """
        Start a background task that automatically resets the cancel timer.
        
        Args:
            timeout: Duration in seconds for the cancel timer
            reset_interval: How often to reset the timer (in seconds)
        
        Returns:
            The asyncio Task that can be cancelled to stop the auto-reset
        """
        async def reset_timer_loop():
            try:
                # Initial setup
                await self.set_cancel_all_orders_after(timeout)
                logger.info(f"Started auto-reset timer: {timeout}s timeout, reset every {reset_interval}s")
                
                while True:
                    await asyncio.sleep(reset_interval)
                    if self.connected():
                        try:
                            result = await self.set_cancel_all_orders_after(timeout)
                            logger.debug(f"Timer reset successful: {result.get('result', {}).get('triggerTime', 'N/A')}")
                        except Exception as e:
                            logger.error(f"Failed to reset cancel timer: {e}")
                            # Don't reconnect, just raise the error
                            raise
                    else:
                        logger.error("WebSocket not connected, cannot reset timer")
                        # Cleanup all tasks when connection is lost
                        try:
                            logger.info("Starting task cleanup due to lost connection in timer task")
                            await cleanup_all_tasks(exclude_current=True, timeout=3.0)
                        except Exception as cleanup_error:
                            logger.error(f"Error during task cleanup: {cleanup_error}")
                        
                        raise KrakenAccountError("WebSocket not connected")
                        
            except asyncio.CancelledError:
                logger.info("Cancel timer auto-reset task cancelled")
                # Try to disable the timer on cancellation
                try:
                    if self.connected():
                        await self.set_cancel_all_orders_after(0)
                        logger.info("Disabled cancel timer on task cancellation")
                except Exception:
                    pass
                raise
            except Exception as e:
                logger.error(f"Error in cancel timer task: {e}")
                # Cleanup tasks on error
                try:
                    logger.info("Starting task cleanup due to timer task error")
                    await cleanup_all_tasks(exclude_current=True, timeout=3.0)
                except Exception as cleanup_error:
                    logger.error(f"Error during task cleanup: {cleanup_error}")
                raise
        
        # Cancel any existing task
        if self._cancel_timer_task and not self._cancel_timer_task.done():
            self._cancel_timer_task.cancel()
            try:
                await self._cancel_timer_task
            except asyncio.CancelledError:
                pass
        
        self._cancel_timer_task = asyncio.create_task(reset_timer_loop())
        return self._cancel_timer_task

    # --- Private Data Subscriptions (v2 format) ---

    async def subscribe_own_trades(self, 
                                 snap_trades: bool = True,
                                 snap_orders: bool = False, 
                                 consolidate_taker: bool = True,
                                 ratecounter: bool = False,
                                 handler: Optional[Callable[[Dict], Awaitable[None]]] = None) -> None:
        """
        Subscribe to own trades data stream using v2 format.
        
        Args:
            snap_trades: If true, the last 50 order fills will be included in snapshot
            snap_orders: If true, open orders will be included in snapshot  
            consolidate_taker: If true, all possible status transitions will be sent
            ratecounter: If true, the rate-limit counter is included in the stream
            handler: Optional callback function to handle execution messages
        """
        if handler:
            self.add_handler('executions', handler)
        
        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "snap_trades": snap_trades,
                "snap_orders": snap_orders,
                "consolidate_taker": consolidate_taker,
                "ratecounter": ratecounter
            }
        }
        
        await self._send_subscription_v2(subscription)
        self._subscriptions['executions'] = subscription
        logger.info(f"Subscribed to executions (snap_trades={snap_trades}, snap_orders={snap_orders}, consolidate_taker={consolidate_taker})")

    async def subscribe_open_orders(self, 
                                  snap_trades: bool = False,
                                  snap_orders: bool = True,
                                  consolidate_taker: bool = True,
                                  ratecounter: bool = False,
                                  handler: Optional[Callable[[Dict], Awaitable[None]]] = None) -> None:
        """
        Subscribe to open orders data stream using v2 format.
        Note: This uses the same 'executions' channel but focuses on orders.
        """
        if handler:
            self.add_handler('executions', handler)
        
        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "snap_trades": snap_trades,
                "snap_orders": snap_orders,
                "consolidate_taker": consolidate_taker,
                "ratecounter": ratecounter
            }
        }
        
        await self._send_subscription_v2(subscription)
        self._subscriptions['executions_orders'] = subscription
        logger.info(f"Subscribed to executions for orders (snap_trades={snap_trades}, snap_orders={snap_orders})")

    async def unsubscribe_own_trades(self) -> None:
        """Unsubscribe from own trades data stream using v2 format."""
        if 'executions' not in self._subscriptions:
            logger.warning("Not subscribed to executions")
            return
        
        unsubscription = {
            "method": "unsubscribe",
            "params": {
                "channel": "executions"
            }
        }
        
        await self._send_subscription_v2(unsubscription)
        self._subscriptions.pop('executions', None)
        self._subscriptions.pop('executions_orders', None)  # Remove both if present
        logger.info("Unsubscribed from executions")

    async def unsubscribe_open_orders(self) -> None:
        """Unsubscribe from open orders data stream using v2 format."""
        # Same as unsubscribe_own_trades since they use the same channel
        await self.unsubscribe_own_trades()

    # --- Account Information Methods ---

    async def get_balance(self) -> Dict[str, str]:
        """
        Retrieves account balance via REST API.
        
        Returns:
            Dict mapping currency codes to balance amounts as strings.
            Example: {'ZUSD': '1000.0000', 'XXBT': '0.50000000'}
        """
        result = await self._make_rest_request(f"/{self.API_VERSION}/private/Balance")
        return result['result']

    # --- Trading Methods (v2 API) ---

    async def add_order_v2(self, symbol: str, side: str, order_type: str, 
                  order_qty: Union[str, float, decimal.Decimal], 
                  limit_price: Optional[Union[str, float, decimal.Decimal]] = None,
                  display_qty: Optional[Union[str, float, decimal.Decimal]] = None, 
                  validate: bool = False, 
                  **kwargs) -> Dict:
        """
        Places orders using the v2 API format.
        Supports all order types including iceberg orders.
        """
        def format_number(value):
            """Ensure numeric values are properly formatted as floats for v2 API"""
            if value is None:
                return None
            if isinstance(value, (str, int, float, decimal.Decimal)):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid numeric value: {value}")
            return value

        payload = {
            "method": "add_order",
            "params": {
                "order_type": order_type,
                "side": side,
                "symbol": symbol,
                "order_qty": format_number(order_qty),
                "validate": validate
            }
        }
        
        if limit_price is not None:
            payload["params"]["limit_price"] = format_number(limit_price)
        
        if display_qty is not None:
            if order_type == "iceberg":
                # Validate minimum display quantity
                try:
                    display_val = float(display_qty)
                    order_val = float(order_qty)
                    min_display = order_val / 15.0
                    if display_val < min_display:
                        raise ValueError(
                            f"Display quantity must be at least {min_display} "
                            f"(1/15 of order quantity)"
                        )
                except (ValueError, TypeError) as e:
                    raise ValueError("Invalid numeric values provided") from e
            payload["params"]["display_qty"] = format_number(display_qty)
        
        payload["params"].update(kwargs)
        return await self._send_request_v2(payload)

    async def amend_order_v2(self, order_id: Optional[str] = None, 
                        cl_ord_id: Optional[str] = None,
                        order_qty: Optional[Union[str, float, decimal.Decimal]] = None,
                        limit_price: Optional[Union[str, float, decimal.Decimal]] = None,
                        display_qty: Optional[Union[str, float, decimal.Decimal]] = None,
                        **kwargs) -> Dict:
        """
        Amends an order using v2 API format.
        """
        if not order_id and not cl_ord_id:
            raise ValueError("Either order_id or cl_ord_id must be provided")
        
        def format_number(value):
            """Ensure numeric values are properly formatted as floats for v2 API"""
            if value is None:
                return None
            if isinstance(value, (str, int, float, decimal.Decimal)):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid numeric value: {value}")
            return value
        
        payload = {
            "method": "amend_order", 
            "params": {}
        }
        
        # Set order identifier
        if order_id:
            payload["params"]["order_id"] = order_id
        if cl_ord_id:
            payload["params"]["cl_ord_id"] = cl_ord_id
        
        # Add parameters to modify
        if order_qty is not None:
            payload["params"]["order_qty"] = format_number(order_qty)
        if limit_price is not None:
            payload["params"]["limit_price"] = format_number(limit_price)
        if display_qty is not None:
            payload["params"]["display_qty"] = format_number(display_qty)
        
        # Validate display_qty if both order_qty and display_qty are provided
        if order_qty is not None and display_qty is not None:
            display_qty_float = float(display_qty)
            order_qty_float = float(order_qty)
            min_display_qty = order_qty_float / 15.0
            if display_qty_float < min_display_qty:
                raise ValueError(f"Display quantity {display_qty} must be at least {min_display_qty:.8f} (1/15 of order quantity)")
        
        # Add any additional parameters
        payload["params"].update(kwargs)
        return await self._send_request_v2(payload)

    async def cancel_order_v2(self, order_id: Optional[str] = None, 
                             cl_ord_id: Optional[str] = None) -> Dict:
        """Cancel an order using v2 API format."""
        if not order_id and not cl_ord_id:
            raise ValueError("Either order_id or cl_ord_id must be provided")
        
        payload = {
            "method": "cancel_order",
            "params": {}
        }
        
        if order_id:
            payload["params"]["order_id"] = order_id
        if cl_ord_id:
            payload["params"]["cl_ord_id"] = cl_ord_id
        
        return await self._send_request_v2(payload)

    async def cancel_all_orders_v2(self) -> Dict:
        """Cancel all orders using v2 API format."""
        payload = {
            "method": "cancel_all_orders",
            "params": {}
        }
        return await self._send_request_v2(payload)

    async def cancel_all_orders_after_v2(self, timeout: int) -> Dict:
        """Cancel all orders after a timeout using v2 API format."""
        payload = {
            "method": "cancel_all_orders_after",
            "params": {
                "timeout": timeout
            }
        }
        return await self._send_request_v2(payload)

    # --- Legacy v1 methods for backward compatibility ---

    async def add_order(self, pair: str, type: str, ordertype: str, volume: str,
                       price: Optional[str] = None, validate: bool = False, **kwargs) -> Dict:
        """
        Legacy method for backward compatibility.
        Converts v1 parameters to v2 format internally.
        """
        # Convert v1 parameters to v2 format
        symbol = pair  # May need symbol mapping
        side = type  # "buy" or "sell"
        order_type = ordertype  # "limit", "market", etc.
        order_qty = volume
        limit_price = price
        
        return await self.add_order_v2(
            symbol=symbol,
            side=side,
            order_type=order_type,
            order_qty=order_qty,
            limit_price=limit_price,
            validate=validate,
            **kwargs
        )

    async def edit_order(self, txid: str, volume: Optional[str] = None,
                        limit_price: Optional[str] = None, **kwargs) -> Dict:
        """Legacy method - edits an existing order."""
        return await self.amend_order_v2(
            order_id=txid,
            order_qty=volume,
            limit_price=limit_price,
            **kwargs
        )

    async def cancel_order(self, order_id: str) -> Dict:
        """Cancel an order using v2 API format (legacy method)."""
        return await self.cancel_order_v2(order_id=order_id)

    async def cancel_all_orders(self) -> Dict:
        """Legacy method - cancels all open orders."""
        return await self.cancel_all_orders_v2()

    # --- Iceberg Order Convenience Methods ---

    async def add_iceberg_order(self, symbol: str, side: str, order_qty: str, 
                               limit_price: str, display_qty: str, 
                               validate: bool = False, **kwargs) -> Dict:
        """Convenience method for placing iceberg orders."""
        return await self.add_order_v2(
            symbol=symbol,
            side=side,
            order_type="iceberg",
            order_qty=order_qty,
            limit_price=limit_price,
            display_qty=display_qty,
            validate=validate,
            **kwargs
        )

    async def amend_iceberg_order(self, order_id: Optional[str] = None, 
                                 cl_ord_id: Optional[str] = None,
                                 order_qty: Optional[str] = None,
                                 limit_price: Optional[str] = None,
                                 display_qty: Optional[str] = None,
                                 **kwargs) -> Dict:
        """Convenience method for amending iceberg orders."""
        return await self.amend_order_v2(
            order_id=order_id,
            cl_ord_id=cl_ord_id,
            order_qty=order_qty,
            limit_price=limit_price,
            display_qty=display_qty,
            **kwargs
        )

    # --- Utility Methods ---

    async def get_open_orders(self) -> Dict:
        """Get open orders via REST API."""
        result = await self._make_rest_request(f"/{self.API_VERSION}/private/OpenOrders")
        return result['result']

    async def get_closed_orders(self) -> Dict:
        """Get closed orders via REST API."""
        result = await self._make_rest_request(f"/{self.API_VERSION}/private/ClosedOrders")
        return result['result']

    async def get_trades_history(self) -> Dict:
        """Get trades history via REST API."""
        result = await self._make_rest_request(f"/{self.API_VERSION}/private/TradesHistory")
        return result['result']

    async def query_orders_info(self, txid: Union[str, List[str]]) -> Dict:
        """Query orders info via REST API."""
        if isinstance(txid, list):
            txid = ','.join(txid)
        
        data = {'txid': txid}
        result = await self._make_rest_request(f"/{self.API_VERSION}/private/QueryOrders", data)
        return result['result']

    async def query_trades_info(self, txid: Union[str, List[str]]) -> Dict:
        """Query trades info via REST API."""
        if isinstance(txid, list):
            txid = ','.join(txid)
        
        data = {'txid': txid}
        result = await self._make_rest_request(f"/{self.API_VERSION}/private/QueryTrades", data)
        return result['result']

    async def close(self) -> None:
        """Closes the WebSocket connection and cleans up resources."""
        try:
            logger.info("Starting KrakenAccount close procedure")
            
            # First, cleanup all asyncio tasks
            try:
                logger.info("Cleaning up all asyncio tasks before closing connections")
                await cleanup_all_tasks(exclude_current=True, timeout=5.0)
            except Exception as cleanup_error:
                logger.error(f"Error during task cleanup in close(): {cleanup_error}")
            
            # Cancel the timer task if running
            if self._cancel_timer_task and not self._cancel_timer_task.done():
                self._cancel_timer_task.cancel()
                try:
                    await self._cancel_timer_task
                except asyncio.CancelledError:
                    pass
            
            # Cancel the message handler task
            if self._message_handler_task and not self._message_handler_task.done():
                self._message_handler_task.cancel()
                try:
                    await self._message_handler_task
                except asyncio.CancelledError:
                    pass
            
            # Close WebSocket connection
            if self._ws_connection:# and not self._ws_connection.closed:
                await self._ws_connection.close()
                logger.info("WebSocket v2 connection closed.")
            
            # Close HTTP session
            if self._session:# and not self._session.closed:
                await self._session.close()
                logger.info("HTTP session closed.")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        finally:
            self._ws_authenticated = False
            self._auth_token = None
            logger.info("KrakenAccount close procedure completed")
    
    async def __aenter__(self) -> 'KrakenAccount':
        await self.connect_v2()
        return self
    
    async def __aexit__(self, exc_type: Optional[type], 
                        exc_val: Optional[Exception], 
                        exc_tb: Optional[Any]) -> None:
        # Cleanup tasks before closing
        try:
            logger.info("Context manager exit: cleaning up tasks")
            await cleanup_all_tasks(exclude_current=True, timeout=3.0)
        except Exception as cleanup_error:
            logger.error(f"Error during context manager task cleanup: {cleanup_error}")
        
        await self.close()


# Example usage and testing
async def main():
    """Example usage of the KrakenAccount class"""
    logging.basicConfig(level=logging.INFO)
    
    # Replace with your actual API credentials
    api_key = "your_api_key"
    api_secret = "your_api_secret"
    
    try:
        # Using async context manager (recommended)
        async with KrakenAccount(api_key, api_secret) as kraken:
            # Get account balance
            balance = await kraken.get_balance()
            print(f"Account balance: {balance}")
            
            # Subscribe to own trades
            async def trade_handler(data):
                print(f"Trade update: {data}")
            
            await kraken.subscribe_own_trades(handler=trade_handler)
            
            # Enable cancel on disconnect
            await kraken.enable_cancel_on_disconnect(timeout=60)
            
            # Place a test order (validation only)
            try:
                result = await kraken.add_order_v2(
                    symbol="BTC/USD",
                    side="buy",
                    order_type="limit",
                    order_qty="0.001",
                    limit_price="30000.00",
                    validate=True  # This will validate but not place the order
                )
                print(f"Order validation result: {result}")
            except Exception as e:
                print(f"Order validation error: {e}")
            
            # Keep the connection alive for a while
            await asyncio.sleep(60)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Run the example
    asyncio.run(main())