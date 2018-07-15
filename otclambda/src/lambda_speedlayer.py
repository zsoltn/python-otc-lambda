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

 Stream processing of Connected Car Data 
Operation:
 1. UTF8 encoded, JSON DATA  received from Kafka in every 1 seconds.
 2. Data converterted to DF and registered TEMP Hive Table 
 3. Alarm SQL executed in current windows dataset  if alarm adding to ALARM TABLE
 4. All data save to HIVE EXTERNAL TABLE (Hive or OBS(S3) for later batch processing  
 Usage: lambda_speedlayer.py
 """
from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SQLContext
from pyspark.sql.types import *
import json
import datetime
from pyspark.sql import HiveContext
from pyspark import SparkFiles

log4jLogger = None 
from lambda_config import Config

config = Config()

def load_resource_file(res_file):
    res_file = os.path.join(os.path.dirname(os.path.realpath(__file__)) , "../conf", res_file )
    with open(res_file, 'r') as tfile:
         ret = tfile.read()
    return ret


def load_resource_file_from_spark_file(res_file_name):
    with open(SparkFiles.get(res_file_name)) as test_file:
        alertsql=test_file.read()
    return alertsql


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Usage: bin/runspeed.sh", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingKafkaLambda")
    log4jLogger = sc._jvm.org.apache.log4j
    logging = log4jLogger.LogManager.getLogger(__name__)
    logging.info("pyspark script logger initialized")

    ssc = StreamingContext(sc, 1)
    
    # gets broksers and topic from config 
    brokers=config.get_lambda_config("kafka_data_producer","broker_list")
    topic=config.get_lambda_config("kafka_data_producer","topic")
    # Streaming context getting data Car JSON data from specific topic 
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])

    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            sqlContext = HiveContext(sc)
            # FIX: memory error Spark 2.0 bug ( < 2.0 )
            sqlContext.setConf("spark.sql.tungsten.enabled","false")

            if rdd.count() < 1:
                return;

            sqlRdd = rdd.map( lambda x: json.loads(x)).map(lambda r: Row( messageid=r["messageid"], messagedate=datetime.datetime.strptime(r["messagedate"], '%Y%m%d%H%M%S'), value=r["value"], metrics=r["metrics"], name=r["name"] ) )
            speedDataFrame = sqlContext.createDataFrame(sqlRdd)

            batch_table_name=config.get_lambda_config(  "lambda_speedlayer","speed_batch_table")
            speedDataFrame.write.mode("append").saveAsTable(batch_table_name)

            # if S3 vals defined then save also to OBS (s3)
            s3_full_path=config.get_lambda_config(  "lambda_speedlayer","s3_full_path")
            if s3_full_path and False:
                speedDataFrame.write.parquet(s3_full_path,mode="append")

            speedDataFrame.show()
            # Creates a temporary view using the DataFrame.
            temp_table_name=config.get_lambda_config(  "lambda_speedlayer","speed_temp_table")
            speedDataFrame.registerTempTable(temp_table_name)

            if __debug__:
                speedDataFrame.printSchema()
                speedDataFrame.head( 10 )
                
            # handling sql alert file
            alertsqlfile=config.get_lambda_config(  "lambda_speedlayer","alert_sql_path")
            
            alertsql = load_resource_file( alertsqlfile )
            # Execute alarm query and get the alam dataset using the temp table
            alertDataFrame = sqlContext.sql(alertsql)
            alertDataFrame.show()
            alertDataFrame.printSchema()

            # save all values to HBASE
            # IF NEED FILTER LATER .filter(lambda x: str(x["metrics"])=='action-credit-limit') \
            # create HBASE mapper
            rowRdd = rdd.map( lambda x: json.loads(x))\
                .map(lambda r: ( str(r["metrics"]) ,[ str(r["name"])+"-"+datetime.datetime.now().strftime("%Y%m%d%H%M%S"), "driver" if "driver" in str(r["metrics"]) else "car", str(r["metrics"]), str(r["value"])  ] ))

            table = config.get_lambda_config(  "lambda_speedlayer","speed_inbox_table")
            host = config.get_lambda_config(  "lambda_speedlayer","hbase_host")
            keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
            valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
            conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
            rowRdd.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
        except Exception as streamerror:
            logging.error( "Stream error:",streamerror )
            print (streamerror)
            raise

    lines.foreachRDD(process)
    lines.pprint()
    ssc.start()
    ssc.awaitTermination()

