# spark_course
Accompanying Notes of Spark Course 


4.4 Uber JVM Profiler

https://portal.influxdata.com/downloads/

vi /etc/influxdb/influxdb.conf
  bind-address = ":8186"

sudo systemctl start influxdb

sudo systemctl status influxdb


https://grafana.com/grafana/download?edition=oss

sudo systemctl daemon-reload

sudo systemctl start grafana-server

sudo systemctl status grafana-server


https://github.com/uber-common/jvm-profiler.git


spark-shell --jars hdfs:/user/hdfs/jvm-profiler-1.0.0.jar --conf spark.executor.extraJavaOptions=-javaagent:jvm-profiler-1.0.0.jar=repom.uber.profiling.reporters.InfluxDBOutputReporter
