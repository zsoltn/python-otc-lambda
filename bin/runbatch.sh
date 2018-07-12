source /opt/client/bigdata_env

EXECFILE="../otclambda/conf/webinar_batchlayer.sql"
spark-sql  -f $EXECFILE
