import pandas as pd
import pika
import time

# RabbitMQ CONNECTION
conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = conn.channel()  # Creating a channel

# Exchange, Routing key name
channel.exchange_declare(
    exchange='WindTurbine_Data_Exchange', exchange_type='topic')
routing_key = 'WindTurbine.Data'

# read CSV file into a pandas df and select first 200 rows
df = pd.read_csv(
    r'C:\Users\Dell\Desktop\Final year project\Dataset\Turbine_Data.csv')
df = df.iloc[:1000]
# print(df)

# Publish each row as seperate message to RabbitMQ
for index, row in df.iterrows():
    Columns = ['Date', 'ActivePower', 'AmbientTemperatue', 'NacellePosition', 'ReactivePower',
               'WTG', 'WindDirection', 'WindSpeed']
    message = row[Columns]
    String_message = str(message)
    print("Index:  " + str(index) + "\n" + "Message :" + String_message)
    channel.basic_publish(exchange='WindTurbine_Data_Exchange',
                          routing_key=routing_key, body=String_message)
    print('Published Message %r: %r' % (routing_key, String_message))
    time.sleep(1)

# Close Conn
conn.close()
