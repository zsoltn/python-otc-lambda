#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Created Date:  June 11 2018
Author: Zsolt Nagy
Version: V1.1 ( Automotive - Lambda example  )
Copyright (c) 2018 T-Systems

 Kafka Sample data generation, '\n' generate sample data every 1 sec based on csv key file ( key data + base value + entropy )
 Usage: kafka_data_producer.py 
 """

from __future__ import print_function
import sys,os
import random
import json

import threading
import logging
import time

from kafka import KafkaConsumer, KafkaProducer
import logging
logging.basicConfig(level=logging.DEBUG)
import csv
import uuid
from datetime import datetime 
from lambda_config import Config

config=Config()
cars=config.get_lambda_config("kafka_data_producer","key_names")
thresholds2 = []

def read_csv(filepath):
    res_file = os.path.join(os.path.dirname(os.path.realpath(__file__)) , "../conf", filepath )
    print( res_file )
    with open(res_file, 'rb') as csv_file:
        reader = csv.DictReader(csv_file, dialect=csv.excel)
        for row in reader:
            thresholds2.append(row)            
    csv_file.close()


def generate_random_data():
    randomcharacterkey = random.sample(cars, 1)[0]
    messageid = str(uuid.uuid1())
    messagedate = datetime.now().strftime("%Y%m%d%H%M%S")
    
    for metrics in thresholds2:        
        data = {"name": randomcharacterkey, "messageid": messageid, "messagedate":messagedate }        
        data["metrics"] = metrics["metrics"]
        random_metric_entropy = random.random() * float( metrics["ent"]) *2 - float(metrics["ent"])
        randommetricval = float(metrics["value"]) + random_metric_entropy
        data["value"] = str(randommetricval)
        yield str(data).replace("'", '"')
class Producer(threading.Thread):
    daemon = True

    def run(self):
        brokers=config.get_lambda_config("kafka_data_producer","broker_list")
        topic=config.get_lambda_config("kafka_data_producer","topic")
        producer = KafkaProducer(bootstrap_servers=brokers) 

        while True:
            for data in generate_random_data():
                #print (json.dumps( data))
                print (data)
                producer.send(topic, data ) 
                time.sleep(1)

def main():
    meta_file=brokers=config.get_lambda_config("kafka_data_producer","meta_file")
    read_csv(meta_file)

    threads = [
        Producer()
        #       , Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(500)


if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
