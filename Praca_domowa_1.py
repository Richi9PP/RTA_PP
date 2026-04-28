from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_timestamps = defaultdict(list)

OKNO_SEKUND = 60
PROG_ALERTU = 3

print("Monitoring anomalii prędkości (>3 transakcje / 60s)...")
print("=" * 60)

for message in consumer:
    tx      = message.value
    user_id = tx['user_id']
    teraz   = datetime.fromisoformat(tx['timestamp'])

    user_timestamps[user_id].append(teraz)

    prog_czasowy = teraz - timedelta(seconds=OKNO_SEKUND)
    user_timestamps[user_id] = [
        t for t in user_timestamps[user_id]
        if t >= prog_czasowy
    ]

    liczba_w_oknie = len(user_timestamps[user_id])

    if liczba_w_oknie > PROG_ALERTU:
        print(f"""
  ╔══════════════════════════════════════════════════╗
  ║  ANOMALIA PRĘDKOŚCI — PODEJRZANA AKTYWNOŚĆ!      ║
  ║  Użytkownik : {user_id:<35} ║
  ║  Transakcja : {tx['tx_id']:<35} ║
  ║  Kwota      : {tx['amount']:<35.2f} ║
  ║  Sklep      : {tx['store']:<35} ║
  ║  W oknie 60s: {liczba_w_oknie:<35} ║
  ╚══════════════════════════════════════════════════╝
""")