"""
TidePool Data Collector
Real-time orderbook and trade data collector for the TidePool dataset.
Maintains full orderbook state and applies incremental updates.
"""
from kraken_api.kraken_ws import KrakenWebSocket
import asyncio
import datetime
import os
import csv
import json
import sys
from typing import Dict, List
from decimal import Decimal

class TidePoolCollector:
    """
    TidePool Data Collector
    Real-time orderbook and trade data collector for the TidePool dataset.
    Maintains full orderbook state in memory.
    """
    def __init__(self, markets: list[str]):
        """
        Initialize the TidePool Collector
        """
        self.markets = markets
        self.client = KrakenWebSocket()
        
        # Dictionary to store full orderbook state for each symbol
        # Format: {symbol: {'bids': {price: qty}, 'asks': {price: qty}}}
        self.orderbooks: Dict[str, Dict[str, Dict[float, float]]] = {}
        
        # Ensure base directory structure exists
        os.makedirs("data", exist_ok=True)
        
        # Set up the message handler
        self.client.on_message = self.handle_message
    
    async def handle_message(self, message):
        """
        Handle incoming WebSocket messages
        """
        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # Handle the new message format
            if isinstance(data, dict) and 'channel' in data and 'type' in data and 'data' in data:
                if data['type'] == 'snapshot':
                    # Initial full orderbook snapshot
                    await self.process_snapshot_message(data)
                elif data['type'] == 'update':
                    await self.process_update_message(data)
            elif isinstance(data, dict) and 'event' in data:
                # Handle other message types (heartbeat, status, etc.)
                print(f"Event: {data['event']}")
                    
        except Exception as e:
            print(f"Error handling message: {e}")
    
    async def process_snapshot_message(self, message: dict):
        """
        Process initial snapshot messages to build full orderbook state
        """
        try:
            channel = message['channel']
            message_data = message['data']
            
            if channel == 'book':
                for item in message_data:
                    symbol = item.get('symbol', '')
                    timestamp = item.get('timestamp', datetime.datetime.utcnow().isoformat() + 'Z')
                    
                    # Initialize orderbook for this symbol
                    self.orderbooks[symbol] = {'bids': {}, 'asks': {}}
                    
                    # Process bids
                    for bid in item.get('bids', []):
                        price = float(bid['price'])
                        qty = float(bid['qty'])
                        self.orderbooks[symbol]['bids'][price] = qty
                    
                    # Process asks
                    for ask in item.get('asks', []):
                        price = float(ask['price'])
                        qty = float(ask['qty'])
                        self.orderbooks[symbol]['asks'][price] = qty
                    
                    print(f"Initialized orderbook for {symbol} with {len(self.orderbooks[symbol]['bids'])} bids and {len(self.orderbooks[symbol]['asks'])} asks")
                    
                    # Save the full orderbook snapshot
                    await self.save_orderbook_snapshot(symbol, timestamp)
                    
        except Exception as e:
            print(f"Error processing snapshot message: {e}")
    
    async def process_update_message(self, message: dict):
        """
        Process update messages and apply changes to orderbook state
        """
        try:
            channel = message['channel']
            message_data = message['data']
            
            # Process each item in the data array
            for item in message_data:
                symbol = item.get('symbol', '')
                timestamp = item.get('timestamp', datetime.datetime.utcnow().isoformat() + 'Z')
                
                if channel == 'book':
                    await self.update_orderbook(symbol, item, timestamp)
                elif channel == 'trade':
                    await self.save_trade_data(symbol, item, timestamp)
                    
        except Exception as e:
            print(f"Error processing update message: {e}")
    
    async def update_orderbook(self, symbol: str, orderbook_item: dict, timestamp: str):
        """
        Update orderbook state with incremental changes and save current state
        """
        try:
            # Ensure we have an orderbook for this symbol
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {'bids': {}, 'asks': {}}
                print(f"Warning: No snapshot received for {symbol}, initializing empty orderbook")
            
            # Process bid updates
            for bid in orderbook_item.get('bids', []):
                price = float(bid['price'])
                qty = float(bid['qty'])
                
                if qty == 0:
                    # Remove this price level
                    self.orderbooks[symbol]['bids'].pop(price, None)
                else:
                    # Update/add this price level
                    self.orderbooks[symbol]['bids'][price] = qty
            
            # Process ask updates
            for ask in orderbook_item.get('asks', []):
                price = float(ask['price'])
                qty = float(ask['qty'])
                
                if qty == 0:
                    # Remove this price level
                    self.orderbooks[symbol]['asks'].pop(price, None)
                else:
                    # Update/add this price level
                    self.orderbooks[symbol]['asks'][price] = qty
            
            # Save the updated orderbook state
            await self.save_orderbook_snapshot(symbol, timestamp)
            
        except Exception as e:
            print(f"Error updating orderbook for {symbol}: {e}")
    
    def get_file_path(self, symbol: str, data_type: str, timestamp: str) -> str:
        """
        Generate file path based on symbol, data type, and timestamp
        """
        # Convert symbol from TUSD/EUR format to TUSD_EUR
        clean_symbol = symbol.replace('/', '_')
        
        # Extract date from timestamp (format: 2025-09-01T20:11:35.911390Z)
        try:
            dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            date_str = dt.strftime("%Y-%m-%d")
        except:
            # Fallback to current date if timestamp parsing fails
            date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        
        # Create directory structure: data/SYMBOL/TYPE/
        directory = f"data/{clean_symbol}/{data_type}"
        os.makedirs(directory, exist_ok=True)
        
        # Return full file path
        return f"{directory}/{date_str}.csv"
    
    async def save_orderbook_snapshot(self, symbol: str, timestamp: str):
        """
        Save current top 25 bids and asks as a single row to CSV
        """
        try:
            if symbol not in self.orderbooks:
                return
                
            filename = self.get_file_path(symbol, 'orderbook', timestamp)
            
            # Create file with headers if it doesn't exist
            file_exists = os.path.exists(filename)
            if not file_exists:
                with open(filename, 'w', newline='') as f:
                    writer = csv.writer(f)
                    # Create headers for 25 bid levels and 25 ask levels
                    headers = ['timestamp']
                    
                    # Add bid headers (25th best to 1st best - furthest to closest)
                    for i in range(25, 0, -1):
                        headers.extend([f'bid_{i}_price', f'bid_{i}_qty'])
                    
                    # Add ask headers (1st best to 25th best - closest to furthest)
                    for i in range(1, 26):
                        headers.extend([f'ask_{i}_price', f'ask_{i}_qty'])
                    
                    writer.writerow(headers)
            
            orderbook = self.orderbooks[symbol]
            
            # Get top 25 bids (highest prices first) and top 25 asks (lowest prices first)
            top_bids = sorted(orderbook['bids'].keys(), reverse=True)[:25]
            top_asks = sorted(orderbook['asks'].keys())[:25]
            
            # Build the row data
            row_data = [timestamp]
            
            # Add bid data (25th best to 1st best)
            for i in range(24, -1, -1):  # 24 down to 0 (reverse order)
                if i < len(top_bids):
                    price = top_bids[i]
                    qty = orderbook['bids'][price]
                    row_data.extend([price, qty])
                else:
                    # No data available for this level
                    row_data.extend([None, None])
            
            # Add ask data (1st best to 25th best)
            for i in range(25):
                if i < len(top_asks):
                    price = top_asks[i]
                    qty = orderbook['asks'][price]
                    row_data.extend([price, qty])
                else:
                    # No data available for this level
                    row_data.extend([None, None])
            
            # Save the complete orderbook state as one row
            with open(filename, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(row_data)
                
            print(f"Saved orderbook snapshot for {symbol}: {len(top_bids)} bids, {len(top_asks)} asks")
            
        except Exception as e:
            print(f"Error saving orderbook snapshot for {symbol}: {e}")
    
    async def save_trade_data(self, symbol: str, trade_item: dict, timestamp: str):
        """
        Save trade data to CSV
        """
        try:
            filename = self.get_file_path(symbol, 'trade', timestamp)
            
            # Create file with headers if it doesn't exist
            file_exists = os.path.exists(filename)
            if not file_exists:
                with open(filename, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['timestamp', 'side', 'price', 'qty', 'ord_type', 'trade_id'])
            
            # Extract trade data
            side = trade_item.get('side', '')
            price = trade_item.get('price', '')
            qty = trade_item.get('qty', '')
            ord_type = trade_item.get('ord_type', '')
            trade_id = trade_item.get('trade_id', '')
            
            # Append data to file
            with open(filename, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    side,
                    price,
                    qty,
                    ord_type,
                    trade_id
                ])
                
            print(f"Saved trade data for {symbol}: {side} {qty} @ {price}")
            
        except Exception as e:
            print(f"Error saving trade data for {symbol}: {e}")
    
    async def run(self) -> None:
        """
        Run the TidePool Collector
        """
        try:
            # Connect to WebSocket
            await self.client.connect()
           
            await self.client.subscribe_book(self.markets, depth=25, handler=self.handle_message)
            await self.client.subscribe_trades(self.markets, handler=self.handle_message)
            await self.client.run()
            await self.client.close()
           
        except Exception as e:
            print(f"Error in run method: {e}")
            sys.exit(1)