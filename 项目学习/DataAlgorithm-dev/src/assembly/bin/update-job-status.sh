#!/bin/bash
taskId=$1
stepId=$2
jobId=$3
logId=$4
batchId=$5
jobServiceUrl=$6
consoleUrl=$7


STATUS_UPDATE_CONTEXT_PATH="/status/update"
STATUS_UPDATE_TASK_INSTANCE="/service/manage/task/updateTaskStatusUpdate"

moduleName="算法引擎"
subModuleName=""
execMsg="异常退出"
status=3



#更新job-engine状态
echo "更新job-engine状态: {\"jobId\":\"${jobId}\",\"logId\":\"${logId}\",\"moduleName\":\"${moduleName}\",\"subModuleName\":\"${subModuleName}\",\"status\":\"${status}\",\"execMsg\":\"${execMsg}\",\"batchTime\":\"${batchId}\"} ${jobServiceUrl}${STATUS_UPDATE_CONTEXT_PATH}"

curl -H "Content-type: application/json" -X POST -d "{\"jobId\":\"${jobId}\",\"logId\":\"${logId}\",\"moduleName\":\"${moduleName}\",\"subModuleName\":\"${subModuleName}\",\"status\":\"${status}\",\"execMsg\":\"${execMsg}\",\"batchTime\":\"${batchId}\"}" ${jobServiceUrl}${STATUS_UPDATE_CONTEXT_PATH}

#更新job-console状态
echo "更新job-console状态：{\"taskId\":\"${taskId}\",\"stepId\":\"${stepId}\",\"batchId\":\"${batchId}\",\"status\":\"${status}\",\"remarks\":\"${execMsg}\"} ${consoleUrl}${STATUS_UPDATE_TASK_INSTANCE}"

curl -H "Content-type: application/json" -X POST -d "{\"taskId\":\"${taskId}\",\"stepId\":\"${stepId}\",\"batchId\":\"${batchId}\",\"status\":\"${status}\",\"remarks\":\"${execMsg}\"}" ${consoleUrl}${STATUS_UPDATE_TASK_INSTANCE}

