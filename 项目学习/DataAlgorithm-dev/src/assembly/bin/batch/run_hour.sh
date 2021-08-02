#!/bin/bash

if [ $# != 5 ] ; then
  echo "参数不对"
  echo "第一个参数为运行起始时间，格式yyyymmddhhmmss"
  echo "第二个参数为运行结束时间, 格式yyyymmddhhmmss"
  echo "第三个参数为碰撞时间分片，单位秒"
  echo "第四个参数为时间压缩间隔, 单位秒"
  echo "第五个参数为关系类型,多个关系用逗号分开。例如:car-car,car-imsi,car-imei,person-imsi"
  exit 0
fi

startD=$1
endD=$2
secondsSeriesThreshold=$3
timeSeriesThreshold=$4
relType=$5

startF=${startD:0:4}"-"${startD:4:2}"-"${startD:6:2}" "${startD:8:2}":"${startD:10:2}":"${startD:12:2}
endF=${endD:0:4}"-"${endD:4:2}"-"${endD:6:2}" "${endD:8:2}":"${endD:10:2}":"${endD:12:2}
echo ${startF}
echo ${endF}



relTypeList=(${relType//,/ })

for relTypeNode in ${relTypeList[@]}
do
  start=`date -d "$startF" +%s`
  i=$start
  end=`date -d "$endF" +%s`
  while [ $i -le $end ]
  do
    date1=`date -d @$i  "+%Y%m%d%H%M%S"`
    echo $date1
    json='{"analyzeModel":{"inputs":[{"cnName":"入库分区数","enName":"numPartitions","desc":"入库分区数","inputVal":"1"},{"cnName":"关系类别","enName":"relType","desc":"关系类别,如：imei-imsi,imsi-imsi","inputVal":"'${relTypeNode}'"},{"cnName":"算法类型","enName":"algorithmType","desc":"算法类型(可选一个或多个，多个用逗号分开),如：lcss,fptree,dtw,entropy","inputVal":"fptree,dtw,entropy,lcss"},{"cnName":"碰撞时间分片","enName":"secondsSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"'${secondsSeriesThreshold}'"},{"cnName":"时间压缩间隔","enName":"timeSeriesThreshold","desc":"碰撞时间分片，单位秒","inputVal":"'${timeSeriesThreshold}'"},{"cnName":"数据库名称","enName":"dataBaseName","desc":"数据库名称","inputVal":"default"},{"cnName":"数据库类型","enName":"databaseType","desc":"数据库类型","inputVal":"hive"},{"cnName":"数据时间范围","enName":"prevHourNums","desc":"获取多长时间的数据，单位是小时","inputVal":"2"},{"cnName":"spark.executor.cores","enName":"spark.executor.cores","desc":"spark.executor.cores","inputVal":"8"}],"isTag":1,"tagInfo":[],"mainClass":"com.suntek.algorithm.process.distribution.Distribution","modelId":8,"modelName":"car-imsi分布","stepId":15,"taskId":12,"descInfo":""},"batchId":'${date1}'}'
    sh ${ALG_DEPLOY_HOME}/bin/batch/data-algorithm.sh "com.suntek.algorithm.process.distribution.Distribution" ${json} "15" "1" "http:"
    echo $json
    let i=3600+$i
  done
done 
exit 0
