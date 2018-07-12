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

 Stream processing of Car Data  UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: lambda_speedlayer.py <broker_list> <topic>
 """
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SQLContext
from pyspark.sql.types import *
import json
import datetime
from pyspark.sql import HiveContext
from pyspark import SparkFiles

log4jLogger = sc._jvm.org.apache.log4j


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: lambda_speedlayer.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingKafkaLambda")

    logging = log4jLogger.LogManager.getLogger(__name__)
    logging.info("pyspark script logger initialized")


    ssc = StreamingContext(sc, 1)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])

    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            sqlContext = HiveContext(sc)
            # FIX: memory error Spark 2.0 bug ( < 2.0 )
            sqlContext.setConf("spark.sql.tungsten.enabled","false")


            # v2.01 spark = SparkSession.builder \
            #.master("local") \
            #.appName("Word Count") \
            #.config("spark.some.config.option", "some-value") \
            #.getOrCreate()
            # Get the singleton instance of SparkSession
            #nzs v1.0 spark = getSparkSessionInstance(rdd.context.getConf())

            if rdd.count() < 1:
                return;

            # Convert RDD[String] to RDD[Row] to DataFrame
            # vs1 
            #wordsDataFrame = sqlContext.read.json( rdd )
            # vs2
            #wordsDataFrame = sqlContext.read.json( rdd.map( lambda x: json.loads(x)) )
            #spark = SparkSession(sc)
            # vs3 
            sqlRdd = rdd.map( lambda x: json.loads(x)).map(lambda r: Row( metrics=r["metrics"], name=r["name"], value=r["value"], value=r["messageid"], value=r["messagedate"] ) )
            wordsDataFrame = sqlContext.createDataFrame(sqlRdd)


            wordsDataFrame.show()
            # Creates a temporary view using the DataFrame.			
            wordsDataFrame.registerTempTable("carstemp")
            # Creates a query and get the alam dataset using the temp table 
            wordCountsDataFrame = sqlContext.sql("select * from  carstemp")
            wordCountsDataFrame.printSchema()


            with open(SparkFiles.get('webinar_streaming.sql')) as test_file:
                alertsql=test_file.read()
                #logging.info(alertsql)

            alertDataFrame = sqlContext.sql(alertsql)			
            alertDataFrame.show()
            alertDataFrame.printSchema()			

            # save all values to HBASE 
            # IF NEED FILTER LATER .filter(lambda x: str(x["metrics"])=='action-credit-limit') \
            # create HBASE mapper 
            rowRdd = rdd.map( lambda x: json.loads(x))\
                .map(lambda r: ( str(r["metrics"]) ,[ str(r["name"])+"-"+datetime.datetime.now().strftime("%Y%m%d%H%M%S"), "action" if str(r["metrics"])=="action-credit-limit" else  "healt", str(r["metrics"]), str(r["value"])] ))
            
            table = 'carsinbox'
            host = 'node-master2-KcVkz'
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

