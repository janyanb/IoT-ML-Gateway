from kafka import KafkaProducer
import json
try:
    import pika
except Exception as e:
    print("Error importing pika module".format(e))

# RMQ Connection
conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = conn.channel()


def serialize_set(set_data):
    return json.dumps(list(set_data)).encode('utf-8')


# Kafka bootsrap server connection
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v.encode('utf-8')  # Stringserializer
)

# declaring queue and exchange
exchange_name = 'WindTurbine_Data_Exchange'
channel.exchange_declare(
    exchange=exchange_name, exchange_type='topic')
channel.queue_declare(queue='All_Data', durable=True)
channel.queue_bind(exchange=exchange_name, queue='All_Data',
                   routing_key='WindTurbine.Data')


def callback(ch, method, properties, body):
    print(f'MQTT: Received message: + \n {body}  ')
    # sending messages to kafka
    string_data = str({body})
    producer.send('WindTurbine_Data_Exchange', value=string_data)
    print('KAFKA: message sent to kafka \n', {body})
 # add stuff about metadata about the sent kafka msg like topic and partitions

    # start consuming messages
channel.basic_consume(
    queue='All_Data', on_message_callback=callback, auto_ack=True)
print('Waiting for messages...')
channel.start_consuming()
