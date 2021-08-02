#!/bin/bash

if [ $# != 4 ] ; then
  echo "参数不对"
  echo "第一个参数为运行起始时间，格式yyyymmddhhmmss"
  echo "第二个参数为运行结束时间, 格式yyyymmddhhmmss"
  echo "第三个参数为同步数据的库名称"
  echo "第四个参数为同步数据的表名称"
  exit 0
fi

startD=$1
endD=$2
databaseName=$3
tableName=$4

startF=${startD:0:4}"-"${startD:4:2}"-"${startD:6:2}" "${startD:8:2}":"${startD:10:2}":"${startD:12:2}
endF=${endD:0:4}"-"${endD:4:2}"-"${endD:6:2}" "${endD:8:2}":"${endD:10:2}":"${endD:12:2}
echo ${startF}
echo ${endF}

start=`date -d "$startF" +%s`
i=$start
end=`date -d "$endF" +%s`
while [ $i -le $end ]
 do
   date1=`date -d @$i  "+%Y%m%d%H%M%S"`
   echo $date1
   json='{"analyzeModel":{"env":{"shellName":"data-algorithm.sh","deployEnvName":"ALG_DEPLOY_HOME"},"inputs":[{"cnName":"spark任务数","enName":"numPartitions","desc":"spark任务数","inputVal":"20"},{"cnName":"spark.executor.cores","enName":"spark.executor.cores","desc":"spark进程核数","inputVal":"5"},{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"default"},{"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"mpp"},{"cnName":"mppUrl","enName":"url","desc":"mppUrl","inputVal":"jdbc:postgresql://172.25.21.18:25308/postgres"},{"cnName":"mpp用户名","enName":"username","desc":"mpp用户名","inputVal":"suntek"},{"cnName":"mpp密码","enName":"password","desc":"mpp密码","inputVal":"suntek@123"},{"cnName":"mpp库名","enName":"landDataBase","desc":"mpp库名","inputVal":"'${databaseName}'"},{"cnName":"同步方式","enName":"method","desc":"同步方式[jdbc，gds]","inputVal":"jdbc"},{"cnName":"hive同步表名","enName":"tableName","desc":"hive同步表名","inputVal":"'${tableName}'"},{"cnName":"mpp同步表名","enName":"mppTableName","desc":"mpp同步表名","inputVal":"'${tableName}'"}],"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.sql.SynchronizeDataFromMPPToHive","modelId":99405,"modelName":"同步mpp数据导hive","stepId":99405,"taskId":99405,"descInfo":""},"batchId":'${date1}'}'
   sh data-algorithm.sh "com.suntek.algorithm.process.sql.SynchronizeDataFromMPPToHive" ${json} "15" "1" "http:"
   echo $json
   let i=3600+$i
 done
