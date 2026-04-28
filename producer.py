from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    tx_num = random.randint(1, 9999)
    return {
        "tx_id":     f"TX{tx_num:04d}",
        "user_id":   f"u{random.randint(1, 20):02d}",
        "amount":    round(random.uniform(5.0, 5000.0), 2),
        "store":     random.choice(["Warszawa", "Kraków", "Gdańsk", "Wrocław"]),
        "category":  random.choice(["elektronika", "odzież", "żywność", "książki"]),
        "timestamp": datetime.now().isoformat()
    }

print("Producent uruchomiony — wysyłam transakcje...")

while True:
    tx = generate_transaction()
    producer.send('transactions', tx)
    print(f"Wysłano: {tx['tx_id']} | {tx['user_id']} | {tx['amount']} PLN | {tx['store']}")
    time.sleep(1)