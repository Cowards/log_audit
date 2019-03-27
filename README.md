# log_audit


## start


```bash

cd /opt/flink/flink && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka -c com.wedata.stream.app.LogAuditFLow  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar

```