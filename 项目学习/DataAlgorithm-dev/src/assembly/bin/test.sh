#!/bin/bash

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../;pwd`
echo ${CURPATH}
cd ${CURPATH}

export HADOOP_CONF_DIR=/etc/hadoop/conf/

JAR_PATH=${CURPATH}/lib
JARS="$JAR_PATH/fastjson-1.2.49.jar,$JAR_PATH/slf4j-api-1.7.25.jar,$JAR_PATH/jest-common-5.3.3.jar,$JAR_PATH/jest-5.3.3.jar,$JAR_PATH/httpcore-nio-4.4.6.jar,$JAR_PATH/httpasyncclient-4.1.3.jar"
#执行模式：默认是yarn-client
execMode=yarn

mainClass="com.suntek.algorithm.process.test.Test"
rel_type="test"
json='yarn#--#{"analyzeModel":{"params":[{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","value":"default"},{"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","value":"hive"}],"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.test.Test","modelId":8,"modelName":"car-imsi分布","stepId":15,"taskId":12,"descInfo":""},"batchId":20200901000000}'
#日志配置文件
driverLogFileName=log4j-driver.properties
executorLogFileName=log4j-executor.properties
driverLog4jPath=${CURPATH}/conf/${driverLogFileName}
executorLog4jPath=${CURPATH}/conf/${executorLogFileName}
echo "driverLog4jPath=${driverLog4jPath}"
echo "executorLog4jPath=${executorLog4jPath}"

starttimestamp=`date +%s`
startdatetime=`date -d @$starttimestamp  "+%Y-%m-%d %H:%M:%S"`
DATA_ALGORITHM=$(ls ${CURPATH}/lib/data-algorithm*.jar)
echo $DATA_ALGORITHM
spark-submit --class com.suntek.algorithm.process.test.Test \
   --master yarn \
   --driver-cores 1 \
   --num-executors 3 \
   --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file://${driverLog4jPath} -DmainClass=${mainClass} -DrelType=${rel_type}" \
   --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=${executorLogFileName}" \
   --conf "spark.shuffle.consolidateFiles=true" \
   --conf "spark.memory.fraction=0.8" \
   --conf "spark.memory.storageFraction=0.3" \
   --conf "spark.shuffle.memoryFraction=0.7" \
   --conf "spark.yarn.executor.memoryOverhead=2g" \
   --conf "spark.task.cpus=2" \
   --conf "spark.driver.memory=1g" \
   --conf "spark.network.timeout=300" \
   --files ${driverLog4jPath},${executorLog4jPath} \
   --jars $JARS \
   $DATA_ALGORITHM "${json}"


endtimestamp=`date +%s`
enddatetime=`date -d @$endtimestamp  "+%Y-%m-%d %H:%M:%S"`

sub_time=`expr $endtimestamp - $starttimestamp`
echo "运行结束，at ${enddatetime}，耗时：${sub_time} s"

