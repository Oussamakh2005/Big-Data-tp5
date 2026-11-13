import json
from kafka import KafkaConsumer
TOPIC = "campus-sensors"
consumer = KafkaConsumer(
  TOPIC,
  bootstrap_servers="localhost:9092",
  auto_offset_reset="earliest",
  enable_auto_commit=True,
  group_id="tp05-campus-monitor",
  value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
print(f"Listening to topic: {TOPIC}")
try:
  counter = [0,0,0,0]
  sum = [0,0,0,0]
  for message in consumer:
    event = message.value
    temperature = event["temperature_c"]
    print(
    f'{event["timestamp"]} | {event["sensor_id"]} | '
    f'T={temperature}°C | RH={event["humidity_pct"]}%',
    f'Campus building : {event["campus_building"]}'
    )
    if temperature >= 35:
      print("ALERT: High temperature detected!")
    counter[event["campus_building"] - 1] = counter[event["campus_building"] - 1]+1
    sum[event["campus_building"] - 1] = sum[event["campus_building"] - 1] + temperature
    avg = sum[event["campus_building"] - 1] / counter[event["campus_building"] - 1]
    print(f"Runnig Average of Compus Building {event["campus_building"]} : {avg:.2f}°C")
except KeyboardInterrupt:
  print("Consumer stopped by user.")
finally:
  consumer.close()
