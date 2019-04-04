# log_audit



## To ALL 

 * 2019年03月29日 `release v1.0` 发布。
 


## start


```bash

cd /opt/log-audit-conn/flink/flink-1.7.2 && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowSupply -c com.wedata.stream.app.LogAuditFlowSupply  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar


cd /opt/log-audit-conn/flink/flink-1.7.2 && sudo -u hdfs  bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowBase -c com.wedata.stream.app.LogAuditFlowBase  /home/log_audit_base-1.0-SNAPSHOT-jar-with-dependencies.jar

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

select * from udt.log_audit_supply_info2;

ALTER TABLE udt.log_audit_supply_info2 ADD IF NOT EXISTS PARTITION (dt='2019-04-03') LOCATION '/tmp/table_temp/log_audit_kafka_flink_supply/dt=2019-04-03';
```

## kafkaTopic

```topic
log_audit_base
log_audit_supply
```


```flink-on-yarn使用流程
1.安装客户端：下载flink的安装包，将安装包上传到要安装JobManager的节点

2.进入Linux系统对安装包进行解压：解压后在节点上配置

3.修改安装目录下conf文件夹内的flink-conf.yaml配置文件，指定JobManager：
 [user@cdh4 conf]# vim flink-conf.yaml
 jobmanager.rpc.address:host
 
4.修改安装目录下conf文件夹内的slave配置文件，指定TaskManager：
 [user@cdh4 conf]# vim slaves
 slavehost
 
5.将配置好的Flink目录分发给其他的flink节点

6.明确虚拟机中已经设置好了环境变量HADOOP_HOME，启动了hdfs和yarn

7.在cdh某节点提交Yarn-Session，使用安装目录下bin目录中的yarn-session.sh脚本进行提交：
 bin/yarn-session.sh -n 2 -s 6 -jm 1024 -tm 1024 -nm test -d
其中：
 -n(--container)：TaskManager的数量。
 -s(--slots)： 每个TaskManager的slot数量，默认一个slot一个core，默认每个taskmanager的slot的个数为1，有时可以多一些taskmanager，做冗余。
 -jm：JobManager的内存（单位MB)。
 -tm：每个taskmanager的内存（单位MB)。
 -nm：yarn 的appName(现在yarn的ui上的名字)。 
 -d：后台执行。
启动后查看Yarn的Web页面，可以看到刚才提交的会话

8.提交Jar到集群运行：
[user@cdh4 flink] cd /opt/log-audit-conn/flink/flink-1.7.2 && 
[user@cdh4 flink] sudo -u hdfs bin/flink run -m yarn-cluster -ynm yarnName -c className  jarPath
下面是实际使用命令
 [user@cdh4 flink]cd /opt/log-audit-conn/flink/flink-1.7.2 && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowSupply -c com.wedata.stream.app.LogAuditFlowSupply  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar
查看结果：
在yarn的resourcemanager的web页面上可以看到正在running的任务，点击右侧的applicationMaster进入flink的web页面可以查看到flink任务执行情况


```
## TODO

* 支持自定义组建扫描日志

* 支持特殊操作告警

* 代码规范