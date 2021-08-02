import sys
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
import json
import logging


from com.suntek.algorithm.evaluation.FileHanlder import FileHanlder

logger = logging.getLogger('py4j')

logger.info("TESTING.....")


def loadData(spark, param):
    batchId = str(param["batchId"])[0:8]
    alg = f'{param["algorithm"]}-{param["aType"]}'
    adSql = "select max(ad) as ad from TB_ACCOMPANY_EVALUATION_RESULT  where bd='{}'  and alg='{}'" \
        .format(batchId, alg)
    print("[最大ad sql]" + adSql)
    ad = spark.sql(adSql).collect()[0]['ad']
    sql = "select * from TB_ACCOMPANY_EVALUATION_RESULT  where bd='{}'  and alg='{}' and ad='{}'" \
        .format(batchId, alg, ad)
    print("[sql]" + sql)
    s = spark.sql(sql)
    return s.toPandas()


def plotChart(d, param, list):
    alg = f'{param["algorithm"]}-{param["aType"]}'
    for elem in list:
        precision = d[(d.evaluation == elem[0])][
            ['thr_or_fpr', 'val_or_tpr']].astype(float)
        ax = precision.plot(kind="scatter", x='thr_or_fpr', y='val_or_tpr', label=elem[1],
                            title=alg)
        ax.set_ylabel(elem[2])
        ax.set_xlabel(elem[3])
        fig = ax.get_figure()
        filePath = f'/root/data-algorithm_liaojp/data/{param["algorithm"]}-{param["aType"]}-{elem[0]}.png'
        fig.savefig(filePath)
        plt.rcParams['font.sans-serif'] = 'SimHei'
        plt.rcParams['axes.unicode_minus'] = False
        fig.savefig(filePath)


def toExcel(d, param, pChart):
    sheets = [t.name for t in pChart]
    df_list = []
    for elem in pChart:
        precision = d[(d.evaluation == elem.name)][
            ['thr_or_fpr', 'val_or_tpr']].astype(float)
        precision.columns = [elem.chart_x,elem.chart_y]
        df_list.append(precision)
    file_path = f'/root/data-algorithm_liaojp/data/{param["algorithm"]}-{param["aType"]}.xlsx'
    FileHanlder.panda_chart(df_list=df_list, cols=1, sheets=sheets, title_x=elem.chart_x, title_y=elem.chart_y, chart_type='scatter',
                            file_path=file_path)


class Chart:
    def __init__(self, name, chart_x, char_y):
        self.name = name
        self.chart_x = chart_x
        self.chart_y = char_y


classifyCharts = [Chart("precision","阈值","查准率"),
                  Chart("recall","阈值","召回率")]
rankCharts = [Chart("precisionAtk","k","准确率"),
              Chart("ndcgAtk","k","NDCG")]


def main(argv):
    print(argv)
    print(argv[1])
    print(argv[1].split("#--#"))
    jstr = json.loads(argv[1].split("#--#")[1])["analyzeModel"]
    param = {}
    for entry in jstr["params"]:
        param[entry["enName"]] = entry["value"]
    del jstr['params']
    param.update(jstr)

    spark = SparkSession.builder \
        .appName('compute_customer_age') \
        .config('spark.executor.memory', '2g') \
        .enableHiveSupport() \
        .getOrCreate()
    d = loadData(spark, param)
    classifyList = [("precision", "查准率分布", "查准率", "阈值"),
                    ("recall", "召回率分布", "召回率", "阈值"),
                    ]
    rankList = [("precisionAtk", "topk准确率分布", "准确率", "k"),
                ("ndcgAtk", "NDCG分布", "NDCG", "k")
                ]
    plist = rankList if (param["evaluationType"] == 'rank') else classifyList
    plotChart(d, param, plist)

    pChart = rankCharts if (param["evaluationType"] == 'rank') else classifyCharts
    toExcel(d,param,pChart)


if __name__ == '__main__':
    main(sys.argv)
    # jstr = 'local#--#{"analyzeModel": {"params": [{"cnName": "算法名", "enName": "algorithm", "desc": "算法名", "value": "LCSS"}' \
    #        ',{"cnName": "类型", "enName": "a_type", "desc": "类型", "value": "IMSI-IMSI"}, {"cnName": "数据库名称", "enName": "dataBaseName",' \
    #        ' "desc": "数据库名称", "value": "default"}, {"cnName": "数据库类型", "enName": "databaseType", "desc": "数据库类型", "value": "hive"},' \
    #        '{"cnName": "评估类型", "enName": "evaluationType", "desc": "评估类型", "value": "rank"},{"cnName": "stat_date", "enName": "stat_date", ' \
    #        '"desc": "stat_date", "value": "20201110"},{"cnName": "pos_id", "enName": "pos_id", "desc": "pos_id", "value": "huadu_1105"}' \
    #        ',{"cnName": "预测结果表", "enName": "pre_table", "desc": "模型预测结果表", "value": "DM_ACCOMPANY_TEST"}], "isTag": 1, "tagInfo"' \
    #        ': [], "mainClass": "com.suntek.algorithm.evaluation.AcompanyEvaluation", "modelId": 100, "modelName": "模型评估", "stepId": 15,' \
    #        ' "taskId": 100, "batchId": 20201105000000, "descInfo": ""}}'
    # main([jstr])
    # jstr = json.loads(str)["analyzeModel"]
    # param ={}
    # for entry in jstr["params"]:
    #     param[entry["enName"]] = entry["value"]
    # del jstr['params']
    # param.update(jstr)
    # print(param)
