from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrich-group',          # inny group_id niż filter!
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def get_risk_level(amount):
    if amount > 3000:
        return "HIGH"
    elif amount > 1000:
        return "MEDIUM"
    else:
        return "LOW"

print("Wzbogacam transakcje o poziom ryzyka...")

for message in consumer:
    tx = message.value
    tx['risk_level'] = get_risk_level(tx['amount'])
    print(f"[{tx['risk_level']:6s}] {tx['tx_id']} | {tx['amount']} PLN | {tx['user_id']}")