from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Struktura: kategoria -> {count, total, min_amt, max_amt}
stats    = defaultdict(lambda: {"count": 0, "total": 0.0,
                                "min": float('inf'), "max": float('-inf')})
msg_count = 0

print("Zbieram statystyki per kategoria...")

for message in consumer:
    tx  = message.value
    cat = tx['category']
    amt = tx['amount']

    stats[cat]['count'] += 1
    stats[cat]['total'] += amt
    stats[cat]['min']    = min(stats[cat]['min'], amt)
    stats[cat]['max']    = max(stats[cat]['max'], amt)
    msg_count           += 1

    if msg_count % 10 == 0:
        print(f"\n{'='*65}")
        print(f"{'Kategoria':<14} {'Liczba':>7} {'Przychód':>12} {'Min':>9} {'Max':>9}")
        print(f"{'-'*65}")
        for cat, s in sorted(stats.items()):
            print(f"{cat:<14} {s['count']:>7} {s['total']:>11.2f}"
                  f" {s['min']:>9.2f} {s['max']:>9.2f}")
        print(f"{'='*65}\n")