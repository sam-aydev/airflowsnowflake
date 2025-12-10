import json
import websocket # pip install websocket-client
from kafka import KafkaProducer

# --- CONFIG ---
SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"
KAFKA_TOPIC = "stock_trades"

# --- KAFKA CONNECTION ---
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# --- FUNCTIONS ---
def on_message(ws, message):
    raw_msg = json.loads(message)
    # print(f"All data of raw_msg: {raw_msg}")
    # --- DEBUGGING BLOCK ---
    # This will show us exactly what keys are in the message
    # If 'e' (event type) is missing, it's a system message.
    if 'e' not in raw_msg:
        print(f"SYSTEM MESSAGE: {raw_msg}")
        return

    # Only process if it is a valid trade
    if raw_msg['e'] == 'trade':
        try:
            data = {
                "ticker": raw_msg['s'],
                "price": float(raw_msg['p']),
                "quantity": float(raw_msg['q']),
                "timestamp": raw_msg['T'],
                # We use .get() here to prevent crashing if 'b' is missing
                "buyer_order_id": raw_msg.get('b', -1),
                "seller_order_id": raw_msg.get('a', -1),
                "trade_id": raw_msg['t']
            }
            print(f" -> {data['ticker']} | {data['price']}")
            producer.send(KAFKA_TOPIC, value=data)
        except Exception as e:
            print(f"PARSING ERROR: {e}")
            print(f"BAD MESSAGE: {raw_msg}")

def on_error(ws, error):
    print(f"ERROR: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")

print(f"Connecting to {SOCKET}...")
ws = websocket.WebSocketApp(
    SOCKET,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()