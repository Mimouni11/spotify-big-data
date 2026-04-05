FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 AS hadoop

FROM bde2020/flume

COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/common/hadoop-common-3.2.1.jar /usr/local/apache-flume/current/lib/
COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/common/lib/ /usr/local/apache-flume/current/lib/
COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/hdfs/hadoop-hdfs-client-3.2.1.jar /usr/local/apache-flume/current/lib/
COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/hdfs/hadoop-hdfs-3.2.1.jar /usr/local/apache-flume/current/lib/

# Replace old Guava with Hadoop's version to fix compatibility
RUN rm -f /usr/local/apache-flume/current/lib/guava-*.jar
COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/common/lib/guava-27.0-jre.jar /usr/local/apache-flume/current/lib/
