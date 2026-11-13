import json
from kafka import KafkaConsumer
TOPIC = "campus-sensors"
consumer = KafkaConsumer(
  TOPIC,
  bootstrap_servers="localhost:9092",
  auto_offset_reset="earliest",
  enable_auto_commit=True,
  group_id="tp05-campus-monitor2",
  value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
print(f"Listening to topic: {TOPIC}")
try:
  counter = 0
  sum = 0
  for message in consumer:
    event = message.value
    temperature = event["temperature_c"]
    print(
    f'{event["timestamp"]} | {event["sensor_id"]} | '
    f'T={temperature}°C | RH={event["humidity_pct"]}%'
    )
    if temperature >= 35:
      print("ALERT: High temperature detected!")
    counter = counter+1
    sum = sum + temperature
    avg = sum / counter
    print(f"Runnig Average : {avg:.2f}°C")
except KeyboardInterrupt:
  print("Consumer stopped by user.")
finally:
  consumer.close()
