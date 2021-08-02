#!/bin/bash

set -e

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../;pwd`
echo ${CURPATH}
cd ${CURPATH}

mainClass=$1
json=$2
jobId=$3
logId=$4
jobServiceUrl=$5

echo "=========================== python ${CURPATH}/bin/parseJson.py '"${json}"'"

value=$(python ${CURPATH}/bin/parseJson.py "${json}")
valueList=(${value//,/ })
echo $value
rel_type=${valueList[0]}
drivermemory=${valueList[1]}
batchId=${valueList[2]}
stepId=${valueList[3]}
taskId=${valueList[4]}
consuleUrl=${valueList[5]}

#rel_type=$(python bin/parseJson.py "${json}")
echo "rel_type:"${rel_type}
echo "drivermemory:"${drivermemory}
echo ${batchId}
echo ${stepId}
echo ${taskId}
echo ${consuleUrl}

function getProperty()
{
    file=$1
    prop_key=$2
    cat $file | grep -w ^${prop_key} | cut -d= -f2
}

logPath=$(getProperty ${CURPATH}/conf/alg-self.conf "driver.log.dir")
if [ -z  "${logPath}"  ]; then
logPath="${CURPATH}"
fi
echo "logPath:"$logPath
curdateTime=$(date "+%Y%m%d%H%M")

algorithmShHome=${CURPATH}/bin

log_dir=$logPath/logs/${rel_type}
log_name=${mainClass}-${rel_type}-${curdateTime}.log
echo "log_dir:"${log_dir}
echo ${log_dir}/${log_name}
mkdir -p -v ${log_dir}/

#注意变量${json}需要用引号括起来避免空格造成参数问题
if [ "${mainClass}" = "com.suntek.algorithm.process.distribution.Distribution"  ]; then
  echo "分布统计"
  echo "${algorithmShHome}/distribute.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} ${json}"
  nohup sh ${algorithmShHome}/distribute.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.fusion.Models" ]; then
  echo "模型融合"
  nohup sh ${algorithmShHome}/modelFusion-handler.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.fusion.FusionRelationHiveToEs" ]; then
  echo "融合分析结果入es"
  nohup sh ${algorithmShHome}/fusion-relation-Es.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.fusion.FusionHandler" ]; then
  echo "模型融合,融合分析结果入es"
  sh ${algorithmShHome}/fusion.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.process.sql.SqlAnalysisPreprocess" ]; then
  echo "所属关系"
  nohup sh ${algorithmShHome}/sql-analysis.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.evaluation.AcompanyEvaluation" ]; then
  echo "模型评估"
  nohup sh ${algorithmShHome}/evaluation.sh  ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.process.sql.SynchronizeDataFromMPPToHive" ]; then
  echo "mpp同步数据到hive"
  nohup sh  ${algorithmShHome}/mpp-hive.sh  ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.process.sql.MppStatistics" ]; then
  echo "人档轨迹统计"
  nohup sh  ${algorithmShHome}/mpp-statistics.sh  ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.process.lifecycle.HiveLifeCycleHandler" ]; then
  echo "数据生命周期管理"
  nohup sh ${algorithmShHome}/lifecycle-control-hive.sh  ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
elif [ "${mainClass}" = "com.suntek.algorithm.process.airport.CredentialsDetectHandler" ]; then
  echo "机场一人多证"
  nohup sh ${algorithmShHome}/java-common.sh  ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
else
  echo "nohup sh ${algorithmShHome}/model.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &"
  nohup sh ${algorithmShHome}/model.sh ${mainClass} ${jobId} ${logId} ${jobServiceUrl} "${json}" ${rel_type} ${drivermemory} ${batchId} ${stepId} ${taskId} ${consuleUrl} 1>${log_dir}/${log_name} 2>&1 &
fi

find ${log_dir}/ -name "${mainClass}*" -mtime +6 -exec rm {} \;
