#!/usr/bin/env python
from __future__ import print_function
import sys,os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SQLContext
from pyspark.sql.types import *
import json
import datetime
from pyspark.sql import HiveContext
from pyspark import SparkFiles
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

def main():
    sc = SparkContext(appName="PythonStreamingKafkaLambda")
    sqlContext = HiveContext(sc)
    # FIX: memory error Spark 2.0 bug ( < 2.0 )
    #sqlContext.setConf("spark.sql.tungsten.enabled","false")
    sqlContext.setConf("hive.metastore.warehouse.dir","/user/hive/warehouse")
    #sqlContext.setConf("hive.metastore.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")


    # v2.01 spark = SparkSession.builder \
    #.master("local") \
    #.appName("Word Count") \
    #.config("spark.some.config.option", "some-value") \
    #.getOrCreate()
    # Get the singleton instance of SparkSession
    #nzs v1.0 spark = getSparkSessionInstance(rdd.context.getConf())

    #jsonStrings = ['{"driver-blood-pressure": "190.857335342", "name": "NRW-8 1234", "current-location-lat": "8.60049239488", "ultrasonic-sensor-front": "0.000943957766966", "current-location-long": "82.687351697", "current-speed": "117.614695851", "ultrasonic-sensor-back": "5.9536578397", "tire-pressure": "2.67141903202"}']
    jsonStrings = ['{"metrics": "driver-blood-pressure", "name": "NRW-8 1234", "value": "190.857335342", "messageid": "myid000943957766966", "messagedate": "201806291700"}']

    rdd = sc.parallelize(jsonStrings)

    if rdd.count() < 1:
        return;

    # Convert RDD[String] to RDD[Row] to DataFrame
    # vs
    #wordsDataFrame = sqlContext.read.json( rdd )
    # vs
    #wordsDataFrame = sqlContext.read.json( rdd.map( lambda x: json.loads(x)) )
    #spark = SparkSession(sc)
    # vs
    #sqlRdd = rdd.map( lambda x: json.loads(x)).map(lambda r: Row( metrics=r["metrics"], name=r["name"], value=r["value"] ) )
    #wordsDataFrame = sqlContext.createDataFrame(sqlRdd)
    # save all data to table for bach processing later
    sqlRdd = rdd.map( lambda x: json.loads(x)).map(lambda r: Row( messageid=r["messageid"], messagedate=datetime.datetime.strptime(r["messagedate"], '%Y%m%d%H%M'), value=r["value"], metrics=r["metrics"], name=r["name"] ) )
    wordsDataFrame = sqlContext.createDataFrame(sqlRdd)

    batch_table_name=config.get_lambda_config(  "lambda_speedlayer","speed_batch_table")
    print(batch_table_name)
    wordsDataFrame.write.mode("append").saveAsTable(batch_table_name)

    # if S3 vals defined then save also to OBS (s3)
    s3_full_path=config.get_lambda_config(  "lambda_speedlayer","s3_full_path")
    if s3_full_path and False:
        wordsDataFrame.write.parquet(s3_full_path,mode="append")

    wordsDataFrame.show()
    # Creates a temporary view using the DataFrame.
    temp_table_name=config.get_lambda_config(  "lambda_speedlayer","speed_temp_table")
    wordsDataFrame.registerTempTable(temp_table_name)


    # Creates a query and get the alam dataset using the temp table
    wordCountsDataFrame = sqlContext.sql("select * from "+temp_table_name)
    wordCountsDataFrame.printSchema()


    # handling sql alert file
    alertsqlfile=config.get_lambda_config(  "lambda_speedlayer","alert_sql_path")
    alertsql = load_resource_file( alertsqlfile )

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



if __name__ == "__main__":
    main()

