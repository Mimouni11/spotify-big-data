#!/bin/bash
# ─────────────────────────────────────────────
#  Sqoop: MySQL → HDFS
#  Usage: docker exec -it spotify_sqoop bash /sqoop/import.sh
# ─────────────────────────────────────────────

MYSQL_HOST="mysql"
MYSQL_DB="spotify_db"
MYSQL_USER="spotify_user"
MYSQL_PASS="spotify_pass"
HDFS_BASE="/data/spotify"
NAMENODE="hdfs://namenode:9000"

CONNECT="jdbc:mysql://${MYSQL_HOST}:3306/${MYSQL_DB}"

# Point Hadoop at the namenode container
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
cat > $HADOOP_CONF_DIR/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${NAMENODE}</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/yarn-site.xml <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>resourcemanager</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/mapred-site.xml <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1</value>
  </property>
</configuration>
EOF

# Clean previous imports if any
hdfs dfs -rm -r -f ${HDFS_BASE}/tracks
hdfs dfs -rm -r -f ${HDFS_BASE}/artists
hdfs dfs -rm -r -f ${HDFS_BASE}/genres

# ── Import tracks ─────────────────────────────
echo "=== Importing tracks ==="
sqoop import \
  --connect "$CONNECT" \
  --username "$MYSQL_USER" \
  --password "$MYSQL_PASS" \
  --table tracks \
  --target-dir "${HDFS_BASE}/tracks" \
  --fields-terminated-by ',' \
  -m 1

# ── Import artists ────────────────────────────
echo "=== Importing artists ==="
sqoop import \
  --connect "$CONNECT" \
  --username "$MYSQL_USER" \
  --password "$MYSQL_PASS" \
  --table artists \
  --target-dir "${HDFS_BASE}/artists" \
  --fields-terminated-by ',' \
  -m 1

# ── Import genres ─────────────────────────────
echo "=== Importing genres ==="
sqoop import \
  --connect "$CONNECT" \
  --username "$MYSQL_USER" \
  --password "$MYSQL_PASS" \
  --table genres \
  --target-dir "${HDFS_BASE}/genres" \
  --fields-terminated-by ',' \
  -m 1

echo "=== All imports done ==="
