try:
    import pika
except Exception as e:
    print("Error importing pika module".format(e))


# RMQ Connection
conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = conn.channel()

# declaring queue and exchange
exchange_name = 'WindTurbine_Data_Exchange'
channel.exchange_declare(
    exchange=exchange_name, exchange_type='topic')
channel.queue_declare(queue='All_Data', durable=True)
channel.queue_bind(exchange=exchange_name, queue='All_Data',
                   routing_key='WindTurbine.Data')


def callback(ch, method, properties, body):
    print(f'Received message: + \n {body}  ')


# start consuming messages
channel.basic_consume(
    queue='All_Data', on_message_callback=callback, auto_ack=True)
print('Waiting for messages...')
channel.start_consuming()
