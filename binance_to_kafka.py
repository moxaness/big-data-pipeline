# binance_to_kafka.py
import json
import signal
import threading
import time
from websocket import WebSocketApp
from kafka import KafkaProducer

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto_trades"

stop_event = threading.Event()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def on_message(ws, message):
    if stop_event.is_set():
        ws.close()
        return

    trade = json.loads(message)
    record = {
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "event_time": trade["T"],
    }

    producer.send(KAFKA_TOPIC, value=record)
    print("Sent:", record)

def on_open(ws):
    print("Connected to Binance WebSocket")

def run_ws():
    ws = WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

def handle_sigint(sig, frame):
    print("\nSIGINT received. Shutting down...")
    stop_event.set()

signal.signal(signal.SIGINT, handle_sigint)

if __name__ == "__main__":
    t = threading.Thread(target=run_ws, daemon=True)
    t.start()

    # Main thread stays responsive to Ctrl+C
    while not stop_event.is_set():
        time.sleep(0.5)

    producer.flush()
    producer.close()
    print("Shutdown complete.")
