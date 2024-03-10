import json
import linecache
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

producer = KafkaProducer(
  bootstrap_servers = "kafka-demo-albertjokelin-45a9.a.aivencloud.com:25104",
  security_protocol="SSL",
  ssl_cafile="ca.pem",
  ssl_certfile="service.cert",
  ssl_keyfile="service.key"
)

print("Generating one unique order every 10 seconds")

for i in range(1, ORDER_LIMIT):
  data = {
    "order_id" : i,
    "user_id" : f"tom_{i}",
    "total_cost": i*10,
    "items" : "burgers, sandwiches, and salads"
  }
  producer.send(
    ORDER_KAFKA_TOPIC,
    json.dumps(data).encode("utf-8")
  )

  print(f"Send data for id {i}")
  time.sleep(5)
