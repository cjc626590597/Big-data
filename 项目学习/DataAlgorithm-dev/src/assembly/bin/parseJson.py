#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import os

import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')

url_xxl = 'http://nacos-center.v-base:30848/nacos/v1/cs/configs?dataId=das&group=das&tenant=a85a37ef-5bec-478c-a60f-0b11f10b3da4'
ROOTPATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_COMPONENTS = 'common.conf'


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


def parseJson(params):
    rel_type = ''
    drivermemory = '1g'
    batchId = '0'
    stepId = '0'
    taskId = '0'
    try:
        params_dic = json.loads(params)
        if 'analyzeModel' in params_dic:
            analyzeModel = params_dic['analyzeModel']
            if 'inputs' in analyzeModel:
                paramsList = analyzeModel['inputs']
                for node in paramsList:
                    if 'enName' in node and 'relType' == node['enName']:
                        rel_type = node['inputVal']
                    if 'enName' in node and 'spark.driver.memory' == node['enName']:
                        drivermemory = node['inputVal']
            if 'stepId' in analyzeModel:
                stepId = str(analyzeModel['stepId'])
            if 'taskId' in analyzeModel:
                taskId = str(analyzeModel['taskId'])
            if 'batchId' in analyzeModel:
                batchId = str(analyzeModel['batchId'])
        if rel_type == '':
            rel_type = stepId
        return rel_type + ',' + drivermemory + ',' + batchId + ',' + stepId + ',' + taskId
    except Exception as e:
        print(e)
        return ''


def getConfConsoleUrl():
    baseProperties = Properties('{}/conf/{}'.format(ROOTPATH, BASE_COMPONENTS)).getPropertiesByFile()
    if 'xxl.console.url' in baseProperties:
        return baseProperties['xxl.console.url']
    return ''


def getConsoleUrl():
    xxl_url = getConfConsoleUrl()
    try:
        xxl_ip = ''
        xxl_port = ''
        xxl_context_path = ''
        rep_xxl = requests.get(url_xxl)
        if rep_xxl.status_code != 200:
            raise Exception
        xxlProperties = Properties(rep_xxl.text).getPropertiesByStr()
        if 'xxl.job.server.ip' in xxlProperties:
            xxl_ip = xxlProperties['xxl.job.server.ip']
        if 'xxl.job.server.port' in xxlProperties:
            xxl_port = xxlProperties['xxl.job.server.port']
        if 'xxl.job.server.context-path' in xxlProperties:
            xxl_context_path = xxlProperties['xxl.job.server.context-path']
        if xxl_ip and xxl_port and xxl_context_path:
            if xxl_context_path.startswith("/"):
                xxl_context_path = xxl_context_path[1: len(xxl_context_path)]
            if xxl_context_path.endswith("/"):
                xxl_context_path = xxl_context_path[0: len(xxl_context_path) - 1]
            xxl_url = "http://{}:{}/{}".format(xxl_ip, xxl_port, xxl_context_path)
    except Exception as e:
        return xxl_url
    return xxl_url


if __name__ == "__main__":
    params = sys.argv[1]
    value = parseJson(params)
    url = getConsoleUrl()
    print(value + ',' + url)

