#!/bin/bash

if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
elif [ `command -v java` ]; then
  RUNNER="java"
else
  echo "JAVA_HOME is not set" >&2
  exit 1
fi

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../;pwd`
echo ${CURPATH}
cd ${CURPATH}

JAR_PATH=${CURPATH}/lib
SPARK_SQL=$(ls $JAR_PATH/spark-sql*.jar)
SPARK_CORE=$(ls $JAR_PATH/spark-core*.jar)
SPARK_NETWORK_COMMON=$(ls $JAR_PATH/spark-network-common*.jar)
SPARK_KVSTORE=$(ls $JAR_PATH/spark-kvstore*.jar)
SPARK_NETWORK_SHUFFLE=$(ls $JAR_PATH/spark-network-shuffle*.jar)
SPARK_UNSAFE=$(ls $JAR_PATH/spark-unsafe*.jar)
SPARK_LAUNCHER=$(ls $JAR_PATH/spark-launcher*.jar)
SPARK_CATALYST=$(ls $JAR_PATH/spark-catalyst*.jar)
PARQUET_HADOOP=$(ls $JAR_PATH/parquet-hadoop-1.10.0.jar)
PARQUET_COMMON=$(ls $JAR_PATH/parquet-common-1.10.0.jar)

DATA_ALGORITHM=$(ls ${CURPATH}/lib/data-algorithm*.jar)
echo $DATA_ALGORITHM
JARS="$DATA_ALGORITHM:$JAR_PATH/fastjson-1.2.49.jar:$JAR_PATH/postgresql-9.4.1212.jar:$JAR_PATH/jedis-2.8.1.jar:$JAR_PATH/commons-pool2-2.4.2.jar:$SPARK_SQL:$SPARK_CORE:$SPARK_NETWORK_COMMON:$SPARK_KVSTORE:$SPARK_NETWORK_SHUFFLE:$SPARK_UNSAFE:$SPARK_LAUNCHER:$SPARK_CATALYST:$PARQUET_HADOOP:$PARQUET_COMMON:$JAR_PATH/snowball-jdbc-2.4.0.jar"
echo $JARS
JAVA_OPTS="-server -Xms4096m -Xmx4096m"
execMode=local
mainClass=$1
jobId=$2
logId=$3
jobServiceUrl=$4
json=$execMode"#--#"$5

starttimestamp=`date +%s`
startdatetime=`date -d @$starttimestamp  "+%Y-%m-%d %H:%M:%S"`

separator='#--#'
MAIN="com.suntek.algorithm.process.main"

$JAVA_HOME/bin/java $JAVA_OPTS -classpath $JARS $MAIN "${jobId}${separator}${logId}${separator}${jobServiceUrl}${separator}${json}"  
sparkexecstatus=$?
echo "======== exit status==${sparkexecstatus} ==========="


endtimestamp=`date +%s`
enddatetime=`date -d @$endtimestamp  "+%Y-%m-%d %H:%M:%S"`

sub_time=`expr $endtimestamp - $starttimestamp`
echo "运行结束，at ${enddatetime}，耗时：${sub_time} s"


if [ "${sparkexecstatus}" -ne "0" ] ;then
echo "${taskId} ${stepId} ${jobId} ${logId}运行第${batchId}个批次异常，at ${enddatetime}"
echo "进行异常退出状态更新....."
sh ${CURPATH}/bin/update-job-status.sh ${taskId} ${stepId} ${jobId} ${logId} ${batchId} ${jobServiceUrl} ${consuleUrl}
exit 1
fi
