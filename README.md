# log_audit


## start


```bash

cd /opt/flink/flink && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka -c com.wedata.stream.app.LogAuditFLow  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar

```



## databases 


```sql

create external table log_audit_base_info(
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
row format delimited fields terminated by "||"
partitioned by (day String);

create external table log_audit_supply_info(
applicationId String,
sql_info String
)
row format delimited fields terminated by "||"
partitioned by (day String);

```