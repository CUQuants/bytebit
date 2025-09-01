"""
TidePool Data Collector
Real-time orderbook and trade data collector for the TidePool dataset.
"""
from kraken_api.kraken_ws import KrakenWebSocket
import asyncio
import datetime
import os
import csv

class TidePoolCollector:
    """
    TidePool Data Collector
    Real-time orderbook and trade data collector for the TidePool dataset.
    """
    def __init__(self, markets: list[str]):
        """
        Initialize the TidePool Collector
        """
        self.markets = markets  # Store the markets parameter
        self.client = KrakenWebSocket()
        # Ensure directory structure exists
        os.makedirs("data/markets/orderbook", exist_ok=True)
    
    async def data_handler(self, data: dict) -> None:
        """
        Handle the data from the Kraken WebSocket
        saves to a csv in data/markets/orderbook/(pair)-(timestamp).csv
        each csv = 24 hours of data
        each csv = 1000 rows
        each row = timestamp, bid_qty, bid_price, ask_qty, ask_price
        """
        timestamp = datetime.datetime.now()
        time_str = timestamp.strftime("%Y-%m-%d")
        time_data = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        # Extract market/pair from the data if available, or iterate through markets
        if 'pair' in data:
            market = data['pair']
            filename = f"data/markets/orderbook/{market}-{time_str}.csv"
            
            # Create file with headers if it doesn't exist
            if not os.path.exists(filename):
                with open(filename, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['timestamp', 'bid_qty', 'bid_price', 'ask_qty', 'ask_price'])
            
            # Write data
            with open(filename, 'a', newline='') as f:
                writer = csv.writer(f)
                # Extract best bid/ask from orderbook data
                if 'bids' in data and data['bids'] and 'asks' in data and data['asks']:
                    best_bid = data['bids'][0] if data['bids'] else ['0', '0']
                    best_ask = data['asks'][0] if data['asks'] else ['0', '0']
                    writer.writerow([
                        time_data,
                        best_bid[1] if len(best_bid) > 1 else '0',  # bid quantity
                        best_bid[0] if len(best_bid) > 0 else '0',  # bid price
                        best_ask[1] if len(best_ask) > 1 else '0',  # ask quantity
                        best_ask[0] if len(best_ask) > 0 else '0'   # ask price
                    ])
        else:
            # Fallback: iterate through all markets (less efficient)
            for market in self.markets:
                filename = f"data/markets/orderbook/{market}-{time_str}.csv"
                
                if not os.path.exists(filename):
                    with open(filename, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(['timestamp', 'bid_qty', 'bid_price', 'ask_qty', 'ask_price'])
                
                with open(filename, 'a', newline='') as f:
                    writer = csv.writer(f)
                    if 'bids' in data and data['bids'] and 'asks' in data and data['asks']:
                        best_bid = data['bids'][0] if data['bids'] else ['0', '0']
                        best_ask = data['asks'][0] if data['asks'] else ['0', '0']
                        writer.writerow([
                            time_data,
                            best_bid[1] if len(best_bid) > 1 else '0',
                            best_bid[0] if len(best_bid) > 0 else '0',
                            best_ask[1] if len(best_ask) > 1 else '0',
                            best_ask[0] if len(best_ask) > 0 else '0'
                        ])
    
    async def run(self) -> None:
        """
        Run the TidePool Collector
        """
        await self.client.run()
        await self.client.subscribe_book(self.markets, self.data_handler)
        await self.client.subscribe_trades(self.markets, self.data_handler)