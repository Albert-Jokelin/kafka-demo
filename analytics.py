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

total_order_count = 0
total_revenue = 0
print("Listening...")
while True:
    for message in consumer:
        print("Updating analytics..")
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message["total_cost"])
        total_order_count += 1
        total_revenue += total_cost
        print(f"Orders today: {total_order_count}")
        print(f"Net revenue today: {total_revenue}")
