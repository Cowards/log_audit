# log_audit



## To ALL 

 * 2019年03月29日 `release v1.0` 发布。
 


## start


```bash

cd /opt/flink/flink && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowSupply -c com.wedata.stream.app.LogAuditFlowSupply  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar


cd /opt/flink/flink && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowBase -c com.wedata.stream.app.LogAuditFlowBase  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar

```



## databases 


```sql
drop table udt.log_audit_base_info;
create table  udt.log_audit_base_info(
applicationId String,
queue String,
submitTime String,
startTime String,
finishTime String,
finalStatus String,
memorySeconds String,
vcoreSeconds String,
applicationType String
)
partitioned by (dt string) 
row format delimited FIELDS TERMINATED by '^'
LINES TERMINATED BY '\n'
stored as textfile location 'hdfs:///tmp/table_temp/log_audit_kafka_flink_base';

ALTER TABLE udt.log_audit_base_info ADD IF NOT EXISTS PARTITION (dt='2019-03-29') LOCATION '/tmp/table_temp/log_audit_kafka_flink_base/dt=2019-03-29';



drop table udt.log_audit_supply_info2;
create table udt.log_audit_supply_info2(
applicationId String,
sql_info String
)partitioned by (dt string) 
row format delimited FIELDS TERMINATED by '^'
LINES TERMINATED BY '\n'
stored as textfile location 'hdfs:///tmp/table_temp/log_audit_kafka_flink_supply';



ALTER TABLE udt.log_audit_supply_info2 ADD IF NOT EXISTS PARTITION (dt='2019-03-29') LOCATION '/tmp/table_temp/log_audit_kafka_flink_supply/dt=2019-03-29';
```

## kafkaTopic

```topic
log_audit_base
log_audit_supply
```


## TODO

* 支持自定义组建扫描日志

* 支持特殊操作告警

* 代码规范