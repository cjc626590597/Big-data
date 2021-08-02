#!/bin/bash

parentDir=$(dirname $0)
path2=$(
  cd $parentDir
  pwd
)
CURPATH=$(
  cd $path2
  cd ../
  pwd
)
echo ${CURPATH}
cd ${CURPATH}

install_dir=$(
    cd $path2
    cd ../../
    pwd
  )
echo "install_dir=${install_dir}"

install() {
    rm -rf /usr/lib/python2.7/site-packages/requests*
    cd ${CURPATH}/soft
    echo "开始安装python"
    cd ${CURPATH}/soft/
    # install pip
    echo "install pip"
    pip -V
    if [ $? -gt 1 ]; then
        tar -xvf setuptools-46.1.3.tar.gz
        cd ${CURPATH}/soft/setuptools-46.1.3
        python setup.py install
        cd ${CURPATH}/soft
        tar -xvf pip-20.0.2.tar.gz
        cd ${CURPATH}/soft/pip-20.0.2
        python setup.py install
    else
        pip uninstall setuptools -y
        cd ${CURPATH}/soft
        tar -xvf setuptools-46.1.3.tar.gz
        cd ${CURPATH}/soft/setuptools-46.1.3
        python setup.py install
    fi

    ## install other
    cd ${CURPATH}/soft/
    pip install certifi-2020.4.5.1.tar.gz
    pip install chardet-3.0.4.tar.gz
    pip install idna-2.9.tar.gz
    pip install urllib3-1.25.8.tar.gz
    pip install requests-2.23.0.tar.gz
}

#环境检查
source /etc/profile
# 检是否已安装java
java -version
if [ $? -gt 1 ];then
    echo "请安装java1.8或以上版本！"
    exit 1
fi

#环境检查
# 检是否已安装python
python -V
if [ $? -gt 1 ];then
    echo "请安装python3或以上版本！"
    exit 1
fi

#环境检查
#spark-submit --version
#if [ $? -gt 1 ];then
#    echo "请安装spark集群对应的spark客户端！"
#    exit 1
#fi

#环境检查
#hive --version
#if [ $? -gt 1 ];then
#    echo "请安装集群对应的hive客户端！"
#    exit 1
#fi

env_flag=0
#安装目录加入环境变量
export_var="export ALG_DEPLOY_HOME=$install_dir/data-algorithm"
export_dir=`cat /etc/profile | grep "export ALG_DEPLOY_HOME="`
if [ -z "${export_dir}" ];then
  env_flag=1
  echo "export ALG_DEPLOY_HOME=$install_dir/data-algorithm" >>/etc/profile
else
  sed -i "/^export ALG_DEPLOY_HOME=/c${export_var}" /etc/profile
fi


#环境检查
scala -version
if [ $? -gt 1 ];then
    echo "安装scala"
    rm -rf /usr/scala
    mkdir -p /usr/scala
    tar zxf soft/scala-2.11.8.tgz -C /usr/scala/
    export_var="export SCALA_HOME=/usr/scala/scala-2.11.8"
    export_scala=`cat /etc/profile | grep "export SCALA_HOME="`
    echo ${export_scala}
    if [ -z "${export_scala}" ];then
        echo "export SCALA_HOME=/usr/scala/scala-2.11.8" >>/etc/profile
    else
        sed -i "/^export SCALA_HOME=/c${export_var}" /etc/profile
    fi
    export_path="$SCALA_HOME/bin:"${export_path}
    echo "export PATH=\$SCALA_HOME/bin:\$PATH:" >>/etc/profile
    echo "修改环境变量"
    env_flag=1
fi

echo "env_flag: ${env_flag}"

if [ ${env_flag} -eq 1 ];then
    echo "source /etc/profile"
    source /etc/profile
fi


# 安装python第三方包
install

cd ${CURPATH}
pwd
# 执行hive初始化sql
#hive -f sh/init_hive_tables.sql
#if [ $? != 0 ]; then
#    echo "安装失败"
#    exit 1
#fi

# 修改配置文件
echo "修改配置文件"
python sh/install.py
if [ $? != 0 ]; then
    echo "安装失败"
    exit 1
fi

cp -f config-template/common.conf conf/common.conf
cp -f config-template/log4j-driver.properties conf/log4j-driver.properties
