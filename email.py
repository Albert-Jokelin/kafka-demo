import json

from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
  ORDER_CONFIRMED_KAFKA_TOPIC,
  bootstrap_servers = "kafka-demo-albertjokelin-45a9.a.aivencloud.com:25104",
  security_protocol="SSL",
  ssl_cafile="ca.pem",
  ssl_certfile="service.cert",
  ssl_keyfile="service.key"
)


emails_sent = set()

while True:
  for message in consumer:
    consumed_message = json.loads(message.value.decode())
    customer_email = consumed_message["customer_email"]

    print(f"Sending an email to {customer_email}")
    emails_sent.add(customer_email)

    print(f"Total unique emails sent: {len(emails_sent)}")
