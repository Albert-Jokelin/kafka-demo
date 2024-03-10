import json
from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRM_KAFKA_TOPIC = "order_confirmed"
consumer = KafkaConsumer(
  ORDER_CONFIRM_KAFKA_TOPIC,
  bootstrap_servers = "kafka-demo-albertjokelin-45a9.a.aivencloud.com:25104",
  auto_offset_reset='earliest',
  security_protocol="SSL",
  ssl_cafile="ca.pem",
  ssl_certfile="service.cert",
  ssl_keyfile="service.key",
  consumer_timeout_ms=1000,
)

producer = KafkaProducer(
  bootstrap_servers = "kafka-demo-albertjokelin-45a9.a.aivencloud.com:25104",
  security_protocol="SSL",
  ssl_cafile="ca.pem",
  ssl_certfile="service.cert",
  ssl_keyfile="service.key"
)
print("Gonna start listening now")

while True:
  for message in consumer:
    print("Ongoing transaction..")
    consumed_message = json.loads(message.value.decode())
    print(consumed_message)

    user_id = consumed_message["user_id"]
    total_cost = consumed_message["total_cost"]

    data = {
      "customer_id": user_id,
      "customer_email": f"{user_id}@abc.com",
      "total_cost": total_cost
    }

    print("Transaction successful...")

    producer.send(
      ORDER_CONFIRM_KAFKA_TOPIC,
      json.dumps(data.encode("utf-8"))
    )

