from kafka import KafkaConsumer
import time
from elasticsearch import Elasticsearch
import json


def publish_to_db(msg_record):

    return True


# Consume messages
consumer = KafkaConsumer('WindTurbine_Data_Exchange',
                         bootstrap_servers=['localhost:9092'])

# Elastic search client
# elasticpass = "Janya420"
# es = Elasticsearch('http://localhost:9200', basic_auth=("Janya", elasticpass))
# 400-index already exists
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.indices.create(
    index='windturbine-data',
    ignore=400
)


es.indices.exists(index='windturbine-data')

final_list = []
for msg in consumer:
    received_object = msg.value.decode('utf-8')
   # print("KAFKA: Received Message \n", received_object)
    message_lines = received_object.split('\\n')
    nan_count = 0
    for line in message_lines:

        # define function to split [message lines] and return

        line = line.strip()
        line_parts = line.split()
        line_list = [line_parts[0], line_parts[1]]
        print(line_list)
        # final_list.append(line_list)

        for list_part in line_list:
            if (list_part[1] == 'NaN'):
                nan_count = nan_count+1

        # time.sleep(3)

    if nan_count < 7:
        # publish to silver kakfa
        print("publish")
        new_line_list = json.dumps(line_list)
        es.index(index='windturbine-data', document=new_line_list)
    else:
        # dont publish
        print("dont publish")
    # store in data influxDB
# print(final_list)
