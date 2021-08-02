#!/bin/bash

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../;pwd`
echo ${CURPATH}
cd ${CURPATH}

JAR_PATH=${CURPATH}/lib

JARS=""
for jar in $JAR_PATH/hive*.jar; do
   i=$[$i+1]
   JARS=$jar,$JARS
done

#
#JAR_PATH=${CURPATH}/lib
#JARS="$JAR_PATH/fastjson-1.2.49.jar"
separator='#--#'
#执行模式：默认是yarn-client
execMode=local

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
echo ${rel_type}
echo ${drivermemory}
echo ${batchId}
echo ${stepId}
echo ${taskId}
echo ${consuleUrl}


#日志配置文件
driverLogFileName=log4j-driver.properties
driverLog4jPath=${CURPATH}/conf/${driverLogFileName}
echo "driverLog4jPath=${driverLog4jPath}"
DATA_ALGORITHM=$(ls ${CURPATH}/lib/data-algorithm*.jar)
echo $DATA_ALGORITHM
spark-submit --class com.suntek.algorithm.process.main \
   --master local[1] \
   --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file://${driverLog4jPath} -DmainClass=${mainClass} -DrelType=${rel_type}" \
   --conf "spark.executor.extraJavaOptions-Dlog4j.configuration=file://${driverLog4jPath} -DmainClass=${mainClass} -DrelType=${rel_type}" \
   --jars $JARS \
   $DATA_ALGORITHM "${jobId}${separator}${logId}${separator}${jobServiceUrl}${separator}${json}"

sparkexecstatus=$?
echo "======== spark-submit exit status==${sparkexecstatus} ==========="

if [ "${sparkexecstatus}" -ne "0" ] ;then
echo "${taskId} ${stepId} ${jobId} ${logId}运行第${batchId}个批次异常，at ${enddatetime}"
echo "进行异常退出状态更新....."
sh ${CURPATH}/bin/update-job-status.sh ${taskId} ${stepId} ${jobId} ${logId} ${batchId} ${jobServiceUrl} ${consuleUrl}
exit 1
fi
