#!/bin/sh

parentDir=`dirname $0`
path2=`cd $parentDir;pwd`
root_dir=`dirname $0`
CURPATH=`cd $path2;cd ../;pwd`

cd ${CURPATH}
# 检查配置文件，变量

count=1
if [ $count -eq 0 ];then
    echo "$name is not running"
    exit 1
fi
