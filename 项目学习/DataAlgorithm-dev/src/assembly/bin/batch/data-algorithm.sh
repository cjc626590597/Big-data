#!/bin/bash

set -e

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../../;pwd`
echo ${CURPATH}
cd ${CURPATH}

mainClass=$1
json=$2
jobId=$3
logId=$4
jobServiceUrl=$5

echo "mainClass=${mainClass}"
echo "jobId=${jobId}"
echo "logId=${logId}"
echo "jobServiceUrl=${jobServiceUrl}"
echo ${json}

value=$(python bin/parseJson.py "${json}")
valueList=(${value//,/ })
echo $value
rel_type=${valueList[0]}
drivermemory=${valueList[1]}
batchId=${valueList[2]}
stepId=${valueList[3]}
taskId=${valueList[4]}
consuleUrl=${valueList[5]}

echo "rel_type="${rel_type}
echo "drivermemory="${drivermemory}
echo "batchId=${batchId}"
echo "stepId=${stepId}"
echo "taskId=${taskId}"
echo "consuleUrl=${consuleUrl}"

function getProperty()
{
    file=$1
    prop_key=$2
    cat $file | grep -w ^${prop_key} | cut -d= -f2
}

logPath=$(getProperty ${CURPATH}/conf/alg-self.conf "driver.log.dir")
echo "logPath:"$logPath
curdateTime=$(date "+%Y%m%d%H%M")
algorithmShHome=${CURPATH}/bin/batch

log_dir=$logPath/logs/${rel_type}

echo "log_dir======${log_dir}"

mkdir -p ${log_dir}

if [ "${mainClass}" = "com.suntek.algorithm.process.distribution.Distribution"  ]; then
  echo "分布统计"
  sh ${algorithmShHome}/distribute.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} ${json} ${rel_type}
elif [ "${mainClass}" = "com.suntek.algorithm.fusion.FusionHandler" ]; then
  echo "模型融合,融合分析结果入es"
  sh ${algorithmShHome}/fusion.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} ${json} ${rel_type}
elif [ "${mainClass}" = "com.suntek.algorithm.process.sql.SqlAnalysisPreprocess" ]; then
  echo "所属关系"
  sh ${algorithmShHome}/sql-analysis.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} ${json} ${rel_type}
else 
  sh ${algorithmShHome}/model.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} ${json} ${rel_type}
fi
