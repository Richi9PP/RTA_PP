from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount  = {}
msg_count     = 0

print("Zliczam transakcje per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']

    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0.0) + tx['amount']
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n{'='*55}")
        print(f"{'Sklep':<12} {'Liczba':>8} {'Suma':>12} {'Średnia':>12}")
        print(f"{'-'*55}")
        for s, count in store_counts.most_common():
            suma    = total_amount[s]
            srednia = suma / count
            print(f"{s:<12} {count:>8} {suma:>11.2f} {srednia:>11.2f}")
        print(f"{'='*55}\n")