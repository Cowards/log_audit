cd /opt/log-audit-conn/flink/flink-1.7.2 && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowSupply -c com.wedata.stream.app.LogAuditFlowSupply  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar

#cd /opt/log-audit-conn/flink/flink-1.7.2 && sudo -u hdfs  bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowBase -c com.wedata.stream.app.LogAuditFlowBase  /home/log_audit_base-1.0-SNAPSHOT-jar-with-dependencies.jar
