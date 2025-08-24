# ByteBit
ByteBit is a robust Python application that streams real-time orderbook and trade data from Kraken's WebSocket API. Built by CU Quants for researchers, traders, and analysts who need reliable, continuous market data collection.

# Data Structure
## Example:
```bash
data/
├── BTC-USD/
│   ├── orderbook/
│   │   └── orderbook_data_2024-06-17.csv
│   └── trades/
│       └── trade_data_2024-06-17.csv
└── ETH-USD/
    ├── orderbook/
    └── trades/
```

## Data Format
```bash
Orderbook CSV: timestamp, 25th_bid_qty, 25th_bid_price, ..., mid_price, best_ask_qty, best_ask_price, ...
```

# System Architecture

## High level
```bash
┌─────────────────────┐
│   Supervisor        │
│   (main.py)         │
│                     │
│ • Manages config    │
│ • Spawns subprocess │
│ • Handles restarts  │
│ • Logs events       │
└──────────┬──────────┘
           │
           │ subprocess
           │
┌──────────▼──────────┐
│   Data Collector    │
│   (collector.py)    │
│                     │
│ • WebSocket client  │
│ • Real-time data    │
│ • CSV file writing  │
│ • Handles sys.exit  │
└─────────────────────┘
```

## Files
```bash
bytebit/
├── README.md
├── requirements.txt
├── config.yaml.example
├── src/
│   ├── main.py              
│   ├── collector.py         
│   ├── config_manager.py    
│   ├── file_manager.py      
│   ├── kraken_client.py     
│   └── utils.py            
├── data/                   
│   ├── BTC-USD/
│   │   ├── orderbook/
│   │   │   └── orderbook_data_2024-06-17.csv
│   │   └── trades/
│   │       └── trade_data_2024-06-17.csv
│   └── ETH-USD/
│       ├── orderbook/
│       └── trades/
└── logs/
    └── bytebit.log
```

