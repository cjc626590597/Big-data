#!/bin/bash

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../;pwd`
echo ${CURPATH}
cd ${CURPATH}

export HADOOP_CONF_DIR=/etc/hadoop/conf/

JAR_PATH=${CURPATH}/lib
JARS="$JAR_PATH/fastjson-1.2.49.jar,$JAR_PATH/slf4j-api-1.7.25.jar,$JAR_PATH/jest-common-5.3.3.jar,$JAR_PATH/jest-5.3.3.jar,$JAR_PATH/httpcore-nio-4.4.6.jar,$JAR_PATH/httpasyncclient-4.1.3.jar,$JAR_PATH/mysql-connector-java-5.1.34.jar"
#执行模式：默认是yarn-client
execMode=yarn

mainClass=$1
jobId=$2
logId=$3
jobServiceUrl=$4
json=$execMode"#--#"$5
rel_type=$6
drivermemory=$7
batchId=$8
stepId=$9
taskId=${10}
consuleUrl=${11}
echo "model.sh 打印"
echo ${rel_type}
echo ${drivermemory}
echo ${batchId}
echo ${stepId}
echo ${taskId}
echo ${consuleUrl}

#日志配置文件
driverLogFileName=log4j-driver.properties
executorLogFileName=log4j-executor.properties
driverLog4jPath=${CURPATH}/conf/${driverLogFileName}
executorLog4jPath=${CURPATH}/conf/${executorLogFileName}
echo "driverLog4jPath=${driverLog4jPath}"
echo "executorLog4jPath=${executorLog4jPath}"

starttimestamp=`date +%s`
startdatetime=`date -d @$starttimestamp  "+%Y-%m-%d %H:%M:%S"`

separator='#--#'
DATA_ALGORITHM=$(ls ${CURPATH}/lib/data-algorithm*.jar)
echo $DATA_ALGORITHM
spark-submit --class com.suntek.algorithm.process.main \
   --master yarn \
   --driver-cores 1 \
   --num-executors 4 \
   --executor-cores 10 \
   --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file://${driverLog4jPath} -DmainClass=${mainClass} -DrelType=${rel_type}" \
   --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=${executorLogFileName}" \
   --conf "spark.shuffle.consolidateFiles=true" \
   --conf "spark.driver.memory=1g" \
   --conf "spark.executor.memory=15g" \
   --conf "spark.memory.fraction=0.8" \
   --conf "spark.memory.storageFraction=0.3" \
   --conf "spark.shuffle.memoryFraction=0.7" \
   --conf "spark.default.parallelism=96" \
   --conf "spark.network.timeout=120000" \
   --conf "spark.sql.shuffle.partitions=2000" \
   --conf "spark.yarn.executor.memoryOverhead=2g" \
   --conf "spark.task.cpus=1" \
   --files ${driverLog4jPath},${executorLog4jPath} \
   --jars $JARS \
   $DATA_ALGORITHM "${jobId}${separator}${logId}${separator}${jobServiceUrl}${separator}${json}"

sparkexecstatus=$?
echo "======== spark-submit exit status==${sparkexecstatus} ==========="

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
