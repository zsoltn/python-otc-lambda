#!/usr/bin/env bash

source /opt/client/bigdata_env
set spark.sql.tungsten.enabled=false

# EXEC CONFIG
CURRENT_PATH=$(pwd)
BASE_PATH="$(dirname "$CURRENT_PATH")"
PACKAGE_NAME=otclambda

DATE_ID=$(date +%F)
LOG_PATH=$BASE_PATH/logs
LOG_FILE=$LOG_PATH/log-${DATE_ID}-${PACKAGE_NAME}.log
EXECUTOR_CORES=1

#EXECFILE=$BASE_PATH/otclambda/src/lamspe.py
EXECFILE=$BASE_PATH/otclambda/src/lambda_speedlayer.py

THIRDPARTY_DIR=$BASE_PATH/3p
#THIRDPARTY_PACKAGE=$THIRDPARTY_DIR/spark-streaming-kafka-assembly_2.10-1.5.1.jar,$THIRDPARTY_DIR/hbase-spark-1.2.0-cdh5.7.2.jar
THIRDPARTY_PACKAGE=$THIRDPARTY_DIR/spark-streaming-kafka-assembly_2.11-1.6.3.jar,$THIRDPARTY_DIR/hbase-spark-1.2.0-cdh5.7.2.jar
DEP_PACKAGE=/opt/client/Spark/spark/jars/spark-core_2.11-2.1.0.jar,/opt/client/Spark/spark/jars/spark-streaming_2.11-2.1.0.jar,/opt/client/Spark/spark/jars/streamingClient/kafka_2.11-0.8.2.1.jar,/opt/client/Spark/spark/jars/streamingClient/kafka-clients-0.8.2.1.jar,/opt/client/Spark/spark/jars/streamingClient/spark-streaming-kafka-0-8_2.11-2.1.0.jar,/opt/client/Spark/spark/examples/jars/spark-examples_2.11-2.1.0.jar,/opt/Bigdata/FusionInsight/hive/spark/lib/spark-examples_2.10-1.5.1.jar

zip -r $BASE_PATH/$PACKAGE_NAME.zip $BASE_PATH/$PACKAGE_NAME

START=$(date +%s.%N)
echo "START: $f - $START" >> $LOG_FILE

spark-submit   --jars "$THIRDPARTY_PACKAGE,$DEP_PACKAGE" --py-files $BASE_PATH/$PACKAGE_NAME.zip $EXECFILE

SUCCESS=$?
END=$(date +%s.%N)
echo "END: $f - $END" >> $LOG_FILE
DIFF=$(echo "$END - $START" | bc -l | sed -e 's/^\./0./' -e 's/^-\./-0./')
echo "$DIFF, $SUCCESS" 

