#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import os

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

ROOTPATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_COMPONENTS = 'alg-self.conf'
CONFPATH = '{}/config-template/'.format(ROOTPATH)
url = 'http://nacos-center.v-base:30848/nacos/v1/cs/configs?dataId=base-components&group=prophet&tenant=a85a37ef-5bec-478c-a60f-0b11f10b3da4'
url_xxl = 'http://nacos-center.v-base:30848/nacos/v1/cs/configs?dataId=das&group=das&tenant=a85a37ef-5bec-478c-a60f-0b11f10b3da4'
SQLPATH = '{}/sql/'.format(ROOTPATH)


class Properties(object):

    def __init__(self, content):
        self.content = content
        self.properties = {}

    def getPropertiesByStr(self):
        try:
            for line in self.content.split('\r\n'):
                line = line.strip().replace('\n', '')
                if line.find("#") != -1:
                    line = line[0:line.find('#')]
                if line.find('=') > 0:
                    strs = line.split('=')
                    strs[1] = line[len(strs[0]) + 1:]
                    self.properties[strs[0].strip()] = strs[1].strip()
        except Exception as e:
            raise e
        return self.properties

    def getPropertiesByFile(self):
        try:
            fopen = open(self.content, 'r')
            for line in fopen:
                line = line.strip()
                if line.find("#") != -1:
                    line = line[0:line.find('#')]
                if line.find('=') > 0:
                    strs = line.split('=')
                    strs[1] = line[len(strs[0]) + 1:]
                    self.properties[strs[0].strip()] = strs[1].strip()
            fopen.close()
        except Exception as e:
            raise e
        return self.properties


class InitDataBase(object):
    def __init__(self, dictProperties, xxlProperties, baseProperties):
        self.dictProperties = dictProperties
        self.xxlProperties = xxlProperties
        self.baseProperties = baseProperties
        self.dictBase = self.getDict()

    def getDict(self):
        dictBase = {'@_ES_ADDRESS_TCP_@': self.getNodeValue('elasticsearch7.nodes.tcp', self.dictProperties),
                    '@_ES_ADDRESS_HTTP_@': self.getNodeValue('elasticsearch7.nodes.http', self.dictProperties)}
        if dictBase['@_ES_ADDRESS_TCP_@'] == '':
            dictBase['@_ES_ADDRESS_TCP_@'] = self.getNodeValue('elasticsearch2.nodes.tcp', self.dictProperties)
        if dictBase['@_ES_ADDRESS_HTTP_@'] == '':
            dictBase['@_ES_ADDRESS_HTTP_@'] = self.getNodeValue('elasticsearch2.nodes.http', self.dictProperties)
        dictBase['@_ES_USERNAME_@'] = self.getNodeValue('elasticsearch7.username', self.dictProperties)
        if dictBase['@_ES_USERNAME_@'] == '':
            dictBase['@_ES_USERNAME_@'] = self.getNodeValue('elasticsearch2.username', self.dictProperties)
        dictBase['@_ES_PASSWORD_@'] = self.getNodeValue('elasticsearch7.password', self.dictProperties)
        if dictBase['@_ES_PASSWORD_@'] == '':
            dictBase['@_ES_PASSWORD_@'] = self.getNodeValue('elasticsearch2.password', self.dictProperties)

        dictBase['@_MYSQL_IP_@'] = self.getNodeValue('mysql.ip', self.dictProperties)
        dictBase['@_MYSQL_PORT_@'] = self.getNodeValue('mysql.port', self.dictProperties)
        dictBase['@_MYSQL_USERNAME_@'] = self.getNodeValue('mysql.username', self.dictProperties)
        dictBase['@_MYSQL_PASSWORD_@'] = self.getNodeValue('mysql.password', self.dictProperties)

        xxl_ip = self.getNodeValue('xxl.job.server.ip', self.xxlProperties)
        xxl_port = self.getNodeValue('xxl.job.server.port', self.xxlProperties)
        xxl_context_path = self.getNodeValue('xxl.job.server.context-path', self.xxlProperties)
        if xxl_context_path.startswith("/"):
            xxl_context_path = xxl_context_path[1: len(xxl_context_path)]
        if xxl_context_path.endswith("/"):
            xxl_context_path = xxl_context_path[0: len(xxl_context_path) - 1]
        xxl_url = "http://{}:{}/{}".format(xxl_ip, xxl_port, xxl_context_path)
        dictBase['@_XXL_CONSOLE_URL_@'] = xxl_url
        print(xxl_url)
        dictBase['@_HIVE_SERVER_NODES_@'] = self.getNodeValue('hive.nodes', self.dictProperties)
        if "driver.log.dir" in baseProperties:
            log_dir = baseProperties["driver.log.dir"]
            if len(log_dir.strip()) == 0 or log_dir == "./":
                log_dir = '{}/'.format(ROOTPATH)
        else:
            log_dir = '{}/'.format(ROOTPATH)
        print(log_dir)
        dictBase['@_DRIVER_LOG_DIR_@'] = log_dir
        print(dictBase)
        return dictBase

    def getNodeValue(self, name, dict):
        if name in dict:
            return dict[name]
        else:
            return ""
            print("warning!!! nacos没有{}配置".format(name))

    def replaceFile(self, fileTemp, filep):
        global line_new
        fr = open(fileTemp, 'r')
        fw = open(filep, 'w+')
        for line in fr:
            line_new = line.encode('utf8')
            for (k, v) in self.dictBase.items():
                line_new = line_new.replace(k, v)
            fw.write('{}'.format(line_new))
        fr.close()
        fw.close()

    def initByProperties(self):
        files = os.listdir(CONFPATH)
        for file in files:
            if not os.path.isdir(file) and file.endswith('.template'):
                conf_temp_path = '{}/{}'.format(CONFPATH, file)
                print(conf_temp_path)
                conf_path = '{}/{}'.format(CONFPATH, file.replace('.template', ''))
                print(conf_path)
                self.replaceFile(conf_temp_path, conf_path)


if __name__ == "__main__":
    try:
        # ES
        print(url)
        rep = requests.get(url)
        if rep.status_code != 200:
            raise Exception
        dictProperties = Properties(rep.text).getPropertiesByStr()

        # xxl 配置
        print(url_xxl)
        rep_xxl = requests.get(url_xxl)
        if rep_xxl.status_code != 200:
            raise Exception
        xxlProperties = Properties(rep_xxl.text).getPropertiesByStr()

        # 读取目录配置文件
        baseProperties = Properties('{}/conf/{}'.format(ROOTPATH, BASE_COMPONENTS)).getPropertiesByFile()
        initDataBase = InitDataBase(dictProperties, xxlProperties, baseProperties)
        # 初始化配置
        initDataBase.initByProperties()
        print('初始化配置 完成')
        sys.exit(0)
    except Exception as e:
        print("初始化配置失败")
        print(e)
        sys.exit(1)