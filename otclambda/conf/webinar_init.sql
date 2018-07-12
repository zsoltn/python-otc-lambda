-- SQL file for Intelligent Car Lambda analisation

-- # in hbase shell 
-- disable  'carinbox'
-- drop 'carinbox'
-- create 'carinbox', ['general']

-- disable  'carsalarm'
-- drop 'carsalarm'
-- create 'carsalarm', ['car', 'driver']

-- disable  'cars'
-- drop 'cars'
-- create 'cars', ['car', 'driver']


drop table meta_lambda_alert;

create external table
    meta_lambda_alert(
        metrics string,
        value string,
        ent string,
        mod string)
row format delimited
fields terminated by ',' 
ESCAPED BY '\\' 
stored as textfile 
location '/tmp/meta_lambda_alert/*';

--CREATE or replace TABLE meta_lambda_alert(   metrics string  ,value   string  ,ent     string ,mod     string);

INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('tire-pressure','3', '0.5','ltgt');
INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('ultrasonic-sensor-front','0.001','0.00009','ltgt');
INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('ultrasonic-sensor-back','4.8','4','ltgt');
INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('driver-blood-pressure','200','200','ltgt');
INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('current-speed','130','50','ltgt');
INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('current-location-long','53.0372285','50','ltgt');
INSERT INTO meta_lambda_alert(metrics,value,ent,mod) VALUES ('current-location-lat','8.6004915','0.000001','ltgt');


drop table spark_inbox;
create external table spark_inbox(
    id string,        
    blood_pressure string,
    tire_pressure string,
    ultrasonic_sensor_front string,
    ultrasonic_sensor_back string,
    current_speed string,
    current_location_long string,
    current_location_lat string,
    value string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"="
    driver:driver-blood-pressure,
    car:tire-pressure,
    car:ultrasonic-sensor-front,
    car:ultrasonic-sensor-back,
    car:current-speed,
    car:current-location-long,
    car:current-location-lat,    
    car:value"
    )
TBLPROPERTIES("hbase.table.name" = "inbox-hbase-lambda");


-- if want to save data to S3 bucket and data folder exsit
drop table lambdas3stable;
CREATE external table lambdas3stable(
  messageid   string
  --,messagedate string  
   ,metrics     string  
  ,name        string 
  ,value       string 
  )
PARTITIONED BY (
  messagedate string
  ) STORED AS parquet 
LOCATION
  "s3a://nzstest/test.parquet";





-- 
-- create table sparkinbox(
--        messageid string,
--        messagedate string,
--        name string,
--        metrics string,
--        value string
-- )
-- TBLPROPERTIES(
-- 'hbaseTableName'='sparkinbox',
-- 'keyCols'='messageid',
-- 'nonKeyCols'='messagedate,s,messagedate;name,s,name; metrics,s,metrics;value,s,value);
