import time
import json
from kafka import KafkaProducer

def run_programmatic_producer():
    print("🛰️ Booting Payment Application API Client Context...")
    
    # Initialize basic serialization options
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    mock_payments = [
        ("acc101", {"txn_id": 1, "amount": 2500, "type": "DEBIT"}),
        ("acc102", {"txn_id": 2, "amount": 99000, "type": "CREDIT"}),
        ("acc101", {"txn_id": 3, "amount": 120, "type": "DEBIT"}),
        ("acc103", {"txn_id": 4, "amount": 45000, "type": "DEBIT"}),
        ("acc101", {"txn_id": 5, "amount": 800, "type": "DEBIT"})
    ]
    
    for account_id, transaction in mock_payments:
        print(f"📤 Forwarding {account_id} payload to 'payments' partition pool...")
        # Key string enforces exact hash partitioning continuity 
        producer.send(topic='payments', key=account_id, value=transaction)
        time.sleep(1.0)
        
    producer.flush()
    print("✅ All testing blocks successfully pushed. Context session gracefully closed.")

if __name__ == "__main__":
    run_programmatic_producer()
