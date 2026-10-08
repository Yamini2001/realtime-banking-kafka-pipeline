import time
import json
import random
import os

def generate_live_traffic():
    # Setup correct matching destination paths
    base_dir = os.path.expanduser("~/connect-demo/source")
    log_path = os.path.join(base_dir, "transactions.log")
    
    cities = ["Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad", "Kolkata"]
    accounts = ["acc201", "acc202", "acc203", "acc204", "acc205", "acc206"]
    txn_id = 106  # Start right after manual lab entries
    
    print(f"🚀 Core Banking Service Started. Appending real-time events to: {log_path}")
    print("Press Ctrl+C to terminate live simulation loop.\n")
    
    while True:
        try:
            payload = {
                "txn_id": txn_id,
                "account": random.choice(accounts),
                "amount": round(random.uniform(10.0, 95000.0), 2),
                "city": random.choice(cities)
            }
            
            # Serialize payload dictionary strictly to inline text
            json_line = json.dumps(payload)
            
            with open(log_path, "a") as log_file:
                log_file.write(json_line + "\n")
                
            print(f"📱 Sent [Txn {txn_id}]: {json_line}")
            
            txn_id += 1
            time.sleep(2.0)  # Streams a brand new record every 2 seconds
            
        except KeyboardInterrupt:
            print("\n🛑 Simulation stopped successfully.")
            break
        except Exception as e:
            print(f"❌ Error occurred: {e}")
            break

if __name__ == "__main__":
    generate_live_traffic()
