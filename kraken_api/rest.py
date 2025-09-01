"""
Kraken REST API Client

A professional Python client for interacting with the Kraken cryptocurrency exchange API.
Provides comprehensive functionality for trading, order management, and market data retrieval.
"""

import hashlib
import hmac
import base64
import time
import urllib.parse
import urllib.request
import json
import logging
import pandas as pd
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import requests


class KrakenAPIError(Exception):
    """Custom exception for Kraken API errors."""
    pass


class KrakenRestClient:
    """
    Professional Kraken REST API client with comprehensive trading functionality.
    
    This client provides a complete interface to Kraken's REST API, including
    market data retrieval, order management, trade execution, and account operations.
    """
    
    def __init__(self, api_key: str = "", api_secret: str = "", 
                 base_url: str = "https://api.kraken.com", 
                 error_messages: bool = True,
                 timeout: int = 30) -> None:
        """
        Initialize the Kraken REST API client.
        
        Args:
            api_key: Kraken API key for authenticated requests
            api_secret: Kraken API secret for authenticated requests
            base_url: Base URL for Kraken API endpoints
            error_messages: Whether to print error messages
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.error_message = error_messages
        self.timeout = timeout
        
        # Setup logging
        self._setup_logging()
        
        # Request session for connection reuse
        self.session = requests.Session()
        self.session.timeout = timeout
        
        self.logger.info("Kraken REST client initialized")

    def _setup_logging(self) -> None:
        """Configure logging for the client."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _get_kraken_signature(self, url_path: str, data: Dict, secret: str) -> str:
        """
        Generate Kraken API signature for authentication.
        
        Args:
            url_path: API endpoint path
            data: Request parameters
            secret: API secret key
            
        Returns:
            Base64 encoded signature
        """
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = url_path.encode() + hashlib.sha256(encoded).digest()
        
        mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    def _kraken_request(self, url_path: str, data: Dict = None, 
                       api_key: str = None, api_secret: str = None) -> Dict:
        """
        Make a request to the Kraken API.
        
        Args:
            url_path: API endpoint path
            data: Request parameters
            api_key: API key for authentication (optional for public endpoints)
            api_secret: API secret for authentication (optional for public endpoints)
            
        Returns:
            API response as dictionary
            
        Raises:
            KrakenAPIError: If the API request fails
        """
        if data is None:
            data = {}
            
        url = self.base_url + url_path
        headers = {'User-Agent': 'Kraken REST API Client'}
        
        # Add authentication for private endpoints
        if api_key and api_secret:
            data['nonce'] = str(int(1000 * time.time()))
            headers['API-Key'] = api_key
            headers['API-Sign'] = self._get_kraken_signature(url_path, data, api_secret)
        
        try:
            if data:
                response = self.session.post(url, headers=headers, data=data)
            else:
                response = self.session.get(url, headers=headers)
                
            response.raise_for_status()
            result = response.json()
            
            # Check for API errors
            if 'error' in result and result['error']:
                error_msg = ', '.join(result['error'])
                self.logger.error(f"Kraken API error: {error_msg}")
                if self.error_message:
                    print(f"Kraken API error: {error_msg}")
                raise KrakenAPIError(error_msg)
                
            return result
            
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            if self.error_message:
                print(f"Request failed: {e}")
            raise KrakenAPIError(f"Request failed: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            if self.error_message:
                print(f"Failed to parse JSON response: {e}")
            raise KrakenAPIError(f"Invalid JSON response: {e}")

    def test_connection(self) -> bool:
        """
        Test connection to Kraken API.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self._kraken_request("/0/public/Time")
            if 'result' in response:
                server_time = response['result']['unixtime']
                self.logger.info(f"Connection successful. Server time: {server_time}")
                if self.error_message:
                    print(f"Connection successful. Server time: {datetime.fromtimestamp(server_time)}")
                return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            if self.error_message:
                print(f"Connection test failed: {e}")
            return False

    def get_bid(self, asset: str = 'XBTUSD', index: int = 0) -> Optional[float]:
        """
        Get bid price for a trading pair.
        
        Args:
            asset: Trading pair (e.g., 'XBTUSD')
            index: Bid level index (0 = best bid)
            
        Returns:
            Bid price as float, None if failed
        """
        try:
            response = self._kraken_request(f"/0/public/Depth?pair={asset}")
            
            if 'result' in response:
                # Get the pair data (Kraken may return different pair format)
                pair_data = list(response['result'].values())[0]
                bids = pair_data.get('bids', [])
                
                if len(bids) > index:
                    bid_price = float(bids[index][0])
                    self.logger.debug(f"Bid price for {asset} at index {index}: {bid_price}")
                    return bid_price
                else:
                    self.logger.warning(f"Bid index {index} not available for {asset}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to get bid for {asset}: {e}")
            if self.error_message:
                print(f"Failed to get bid for {asset}: {e}")
            return None

    def get_ask(self, asset: str = 'XBTUSD', index: int = 0) -> Optional[float]:
        """
        Get ask price for a trading pair.
        
        Args:
            asset: Trading pair (e.g., 'XBTUSD')
            index: Ask level index (0 = best ask)
            
        Returns:
            Ask price as float, None if failed
        """
        try:
            response = self._kraken_request(f"/0/public/Depth?pair={asset}")
            
            if 'result' in response:
                # Get the pair data (Kraken may return different pair format)
                pair_data = list(response['result'].values())[0]
                asks = pair_data.get('asks', [])
                
                if len(asks) > index:
                    ask_price = float(asks[index][0])
                    self.logger.debug(f"Ask price for {asset} at index {index}: {ask_price}")
                    return ask_price
                else:
                    self.logger.warning(f"Ask index {index} not available for {asset}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to get ask for {asset}: {e}")
            if self.error_message:
                print(f"Failed to get ask for {asset}: {e}")
            return None

    def get_orderbook(self, pair: str) -> Optional[Dict]:
        """
        Get order book data for a trading pair.
        
        Args:
            pair: Trading pair (e.g., 'XBTUSD')
            
        Returns:
            Order book data with bids and asks, None if failed
        """
        try:
            response = self._kraken_request(f"/0/public/Depth?pair={pair}")
            
            if 'result' in response:
                # Return the order book data
                pair_data = list(response['result'].values())[0]
                return {
                    'pair': pair,
                    'bids': pair_data.get('bids', []),
                    'asks': pair_data.get('asks', []),
                    'timestamp': time.time()
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get orderbook for {pair}: {e}")
            if self.error_message:
                print(f"Failed to get orderbook for {pair}: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a specific order.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            True if successful, False otherwise
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for order cancellation")
            if self.error_message:
                print("API credentials required for order cancellation")
            return False
            
        try:
            params = {'txid': order_id}
            response = self._kraken_request(
                "/0/private/CancelOrder", 
                params, 
                self.api_key, 
                self.api_secret
            )
            
            if 'result' in response:
                self.logger.info(f"Successfully cancelled order: {order_id}")
                if self.error_message:
                    print(f"Successfully cancelled order: {order_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            if self.error_message:
                print(f"Failed to cancel order {order_id}: {e}")
            return False

    def edit_order(self, txid: str, pair: str, side: str, price: float, 
                   volume: float, new_userref: Optional[int] = None) -> Optional[Dict]:
        """
        Edit an existing order.
        
        Args:
            txid: Transaction ID of the order to edit
            pair: Trading pair
            side: 'buy' or 'sell'
            price: New order price
            volume: New order volume
            new_userref: New user reference ID
            
        Returns:
            Edit response data, None if failed
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for order editing")
            if self.error_message:
                print("API credentials required for order editing")
            return None
            
        try:
            params = {
                'txid': txid,
                'pair': pair,
                'type': side,
                'ordertype': 'limit',
                'price': str(price),
                'volume': str(volume)
            }
            
            if new_userref is not None:
                params['userref'] = str(new_userref)
            
            response = self._kraken_request(
                "/0/private/EditOrder", 
                params, 
                self.api_key, 
                self.api_secret
            )
            
            if 'result' in response:
                self.logger.info(f"Successfully edited order: {txid}")
                return response['result']
                
        except Exception as e:
            self.logger.error(f"Failed to edit order {txid}: {e}")
            if self.error_message:
                print(f"Failed to edit order {txid}: {e}")
            return None

    def get_open_orders(self, pair: Optional[str] = None) -> Dict:
        """
        Get open orders, optionally filtered by trading pair.
        
        Args:
            pair: Trading pair to filter by (optional)
            
        Returns:
            Dictionary containing open orders data
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for getting open orders")
            return {
                'error': ['API credentials required'],
                'result': {}
            }
            
        try:
            params = {}
            if pair:
                params['pair'] = pair
                
            response = self._kraken_request(
                "/0/private/OpenOrders", 
                params, 
                self.api_key, 
                self.api_secret
            )
            
            return response
            
        except Exception as e:
            self.logger.error(f"Failed to get open orders: {e}")
            return {
                'error': [str(e)],
                'result': {}
            }

    def get_my_recent_orders(self, pair: Optional[str] = None, 
                           since: Optional[str] = None, 
                           count: Optional[int] = None, 
                           userref: Optional[str] = None) -> Union[pd.DataFrame, bool]:
        """
        Get recent order history.
        
        Args:
            pair: Trading pair to filter by
            since: Starting timestamp
            count: Maximum number of results
            userref: User reference filter
            
        Returns:
            DataFrame with order data or False if failed
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for getting order history")
            if self.error_message:
                print("API credentials required for getting order history")
            return False
            
        try:
            params = {}
            if pair:
                params['pair'] = pair
            if since:
                params['start'] = since
            if count:
                params['end'] = str(count)
            if userref:
                params['userref'] = userref
                
            response = self._kraken_request(
                "/0/private/ClosedOrders", 
                params, 
                self.api_key, 
                self.api_secret
            )
            
            if 'result' in response and 'closed' in response['result']:
                orders = response['result']['closed']
                
                if not orders:
                    return pd.DataFrame()
                
                # Convert to DataFrame
                order_data = []
                for order_id, order_info in orders.items():
                    order_data.append({
                        'order_id': order_id,
                        'pair': order_info.get('descr', {}).get('pair', ''),
                        'side': order_info.get('descr', {}).get('type', ''),
                        'price': float(order_info.get('descr', {}).get('price', 0)),
                        'volume': float(order_info.get('vol', 0)),
                        'status': order_info.get('status', ''),
                        'time': datetime.fromtimestamp(order_info.get('opentm', 0)),
                        'userref': order_info.get('userref', ''),
                        'fee': float(order_info.get('fee', 0))
                    })
                
                return pd.DataFrame(order_data)
                
        except Exception as e:
            self.logger.error(f"Failed to get recent orders: {e}")
            if self.error_message:
                print(f"Failed to get recent orders: {e}")
            return False

    def get_my_recent_trades(self, pair: Optional[str] = None, 
                           since: Optional[str] = None, 
                           count: Optional[int] = None) -> Union[pd.DataFrame, bool]:
        """
        Get recent trade history.
        
        Args:
            pair: Trading pair to filter by
            since: Starting timestamp  
            count: Maximum number of results
            
        Returns:
            DataFrame containing trade history with columns:
                - trade_id: Trade ID
                - pair: Trading pair
                - side: 'buy' or 'sell'
                - price: Trade price
                - volume: Trade volume
                - time: Trade timestamp (datetime)
                - cost: Trade cost
                - fee: Trade fee
                - margin: Margin (if applicable)
                - misc: Miscellaneous info
            bool: False if the operation fails
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for getting trade history")
            if self.error_message:
                print("API credentials required for getting trade history")
            return False
            
        try:
            params = {}
            if pair:
                params['pair'] = pair
            if since:
                params['start'] = since
            if count:
                params['end'] = str(count)
                
            response = self._kraken_request(
                "/0/private/TradesHistory", 
                params, 
                self.api_key, 
                self.api_secret
            )
            
            if 'result' in response and 'trades' in response['result']:
                trades = response['result']['trades']
                
                if not trades:
                    return pd.DataFrame()
                
                # Convert to DataFrame
                trade_data = []
                for trade_id, trade_info in trades.items():
                    trade_data.append({
                        'trade_id': trade_id,
                        'pair': trade_info.get('pair', ''),
                        'side': trade_info.get('type', ''),
                        'price': float(trade_info.get('price', 0)),
                        'volume': float(trade_info.get('vol', 0)),
                        'time': datetime.fromtimestamp(trade_info.get('time', 0)),
                        'cost': float(trade_info.get('cost', 0)),
                        'fee': float(trade_info.get('fee', 0)),
                        'margin': float(trade_info.get('margin', 0)),
                        'misc': trade_info.get('misc', '')
                    })
                
                df = pd.DataFrame(trade_data)
                df = df.sort_values('time', ascending=False)
                return df
                
        except Exception as e:
            self.logger.error(f"Failed to get recent trades: {e}")
            if self.error_message:
                print(f"Failed to get recent trades: {e}")
            return False

    def cancel_all_open_orders(self, pair: str) -> Dict:
        """
        Cancel all open orders for a specific trading pair.
        
        Args:
            pair: Trading pair to cancel orders for
            
        Returns:
            Dictionary with cancellation results
        """
        # First get all open orders for the pair
        open_orders_response = self.get_open_orders(pair)
        
        # Check if the request was successful
        if 'error' in open_orders_response and open_orders_response['error']:
            return open_orders_response
        
        # Extract order IDs from the response
        open_orders = open_orders_response.get('result', {}).get('open', {})
        
        if not open_orders:
            return {
                'error': [],
                'result': {
                    'count': 0,
                    'message': f'No open orders found for pair {pair}'
                }
            }
        
        # Collect all order IDs for the specified pair
        order_ids = []
        for order_id, order_data in open_orders.items():
            # Double-check the pair matches (in case API filtering didn't work perfectly)
            order_pair = order_data.get('descr', {}).get('pair', '')
            if order_pair == pair or not pair:  # Include all if pair filtering failed
                order_ids.append(order_id)
        
        if not order_ids:
            return {
                'error': [],
                'result': {
                    'count': 0,
                    'message': f'No open orders found for pair {pair}'
                }
            }
        
        # Cancel orders one by one (more reliable than batch)
        cancelled_orders = []
        failed_orders = []
        
        for order_id in order_ids:
            cancel_params = {
                'txid': order_id
            }
            
            try:
                cancel_response = self._kraken_request(
                    "/0/private/CancelOrder", 
                    cancel_params, 
                    self.api_key, 
                    self.api_secret
                )
                
                if 'error' in cancel_response and cancel_response['error']:
                    failed_orders.append({
                        'order_id': order_id,
                        'error': cancel_response['error']
                    })
                else:
                    cancelled_orders.append(order_id)
                    
            except Exception as e:
                failed_orders.append({
                    'order_id': order_id,
                    'error': str(e)
                })
        
        # Return summary response
        return {
            'error': [],
            'result': {
                'cancelled_count': len(cancelled_orders),
                'failed_count': len(failed_orders),
                'cancelled_orders': cancelled_orders,
                'failed_orders': failed_orders,
                'pair': pair
            }
        }

    def cancel_all_orders(self, asset: Optional[str] = None) -> Union[Dict, bool]:
        """
        Cancel all open orders for a specific asset or all assets.
        
        Args:
            asset: Trading pair to cancel orders for (e.g., 'XBTUSD'). 
                  If None, cancels all orders for all assets.
        
        Returns:
            Dictionary containing:
                - success: List of successfully cancelled order IDs
                - failed: List of failed order IDs with error messages
                - total_cancelled: Number of successfully cancelled orders
                - total_failed: Number of failed cancellations
            bool: False if unable to get open orders
        """
        try:
            # Get all open orders for the specified asset
            orders_df = self.get_my_recent_orders(pair=asset)
            
            if orders_df is False:
                if self.error_message:
                    print("KrakenRestClient.cancel_all_orders: Failed to get open orders")
                return False
            
            if orders_df.empty:
                if self.error_message:
                    print(f"KrakenRestClient.cancel_all_orders: No open orders found for {asset or 'any asset'}")
                return {
                    'success': [],
                    'failed': [],
                    'total_cancelled': 0,
                    'total_failed': 0
                }
            
            # Initialize result tracking
            successful_cancellations = []
            failed_cancellations = []
            
            # Cancel each order
            for _, order in orders_df.iterrows():
                order_id = order['order_id']
                try:
                    cancel_result = self.cancel_order(order_id)
                    
                    if cancel_result is not False:
                        successful_cancellations.append(order_id)
                        if self.error_message:
                            print(f"Successfully cancelled order: {order_id}")
                    else:
                        failed_cancellations.append({
                            'order_id': order_id,
                            'error': 'Cancel operation returned False'
                        })
                        if self.error_message:
                            print(f"Failed to cancel order: {order_id}")
                            
                except Exception as e:
                    failed_cancellations.append({
                        'order_id': order_id,
                        'error': str(e)
                    })
                    if self.error_message:
                        print(f"Exception while cancelling order {order_id}: {e}")
            
            result = {
                'success': successful_cancellations,
                'failed': failed_cancellations,
                'total_cancelled': len(successful_cancellations),
                'total_failed': len(failed_cancellations)
            }
            
            if self.error_message:
                print(f"Cancel all orders completed. Cancelled: {result['total_cancelled']}, Failed: {result['total_failed']}")
            
            return result
            
        except Exception as e:
            if self.error_message:
                print(f"KrakenRestClient.cancel_all_orders: {e}")
            return False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close session."""
        if hasattr(self, 'session'):
            self.session.close()


def main():
    """Example usage of the KrakenRestClient."""
    # Initialize client (add your API credentials for authenticated endpoints)
    client = KrakenRestClient(
        api_key="your_api_key_here",
        api_secret="your_api_secret_here",
        error_messages=True
    )
    
    try:
        # Test connection
        print("Testing connection...")
        if client.test_connection():
            print("✓ Connection successful")
        
        # Get market data (no auth required)
        print("\nGetting market data...")
        bid = client.get_bid('XBTUSD')
        ask = client.get_ask('XBTUSD')
        print(f"BTC/USD - Bid: ${bid}, Ask: ${ask}")
        
        # Get orderbook
        orderbook = client.get_orderbook('XBTUSD')
        if orderbook:
            print(f"Order book has {len(orderbook['bids'])} bids and {len(orderbook['asks'])} asks")
        
        # For authenticated endpoints, uncomment if you have valid credentials:
        # orders_df = client.get_my_recent_orders()
        # trades_df = client.get_my_recent_trades()
        # print(f"Found {len(orders_df)} recent orders" if not orders_df.empty else "No recent orders")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()