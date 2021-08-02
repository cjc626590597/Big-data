package com.suntek.algorithm.process.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.suntek.algorithm.common.http.HttpUtil
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import io.searchbox.indices.mapping.GetMapping
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions


//noinspection SpellCheckingInspection
object EsUtil {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val CONNTIMEOUT = 100000000
  val READTIMEOUT = 100000000
  val ISMULTITHREADED = true
  @throws[Exception]
  def getJestClient(esNodesHttp: String): JestClient = {

    logger.info(s"esNodesHttp: $esNodesHttp")
    try {
      val factory = new JestClientFactory
      val builder = new HttpClientConfig.Builder(s"http://$esNodesHttp")
                                        .connTimeout(CONNTIMEOUT)
                                        .readTimeout(READTIMEOUT)
                                        .multiThreaded(ISMULTITHREADED)
                                        .build
      factory.setHttpClientConfig(builder)
      factory.getObject
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        throw ex
    }
  }

  def close(jestClient: JestClient): Unit = jestClient.close()

  @throws[Exception]
  def getTypeName(jestClient: JestClient,
                  indexName: String): String = {

    logger.info(s"indexName: $indexName")
    var typeName = "_doc"
    try {
      val action = new GetMapping.Builder().addIndex(indexName).build
      val rest = jestClient.execute(action)
      val mappingObject = rest.getJsonObject.get(indexName).getAsJsonObject.get("mappings").getAsJsonObject
      import scala.collection.JavaConversions._
      for (map <- mappingObject.entrySet) {
        val key = map.getKey
        if (key != "properties") {
          typeName = key
        }
      }
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        throw ex
    } finally {
      logger.info(s"typeName: $typeName")
    }
    typeName
  }

  @throws[Exception]
  def insertBatch(esNodesHttp: String,
                  indexName: String,
                  typeName: String,
                  data: List[(String, String)],
                  batchSize: Int = 5000): Unit = {

    if (data.nonEmpty) {
      var bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName)
      var i = 0
      val jestClient = EsUtil.getJestClient(esNodesHttp)
      data.foreach { r =>
        val index = new Index.Builder(r._2)
        index.id(r._1)
        bulk.addAction(index.build())
        i += 1
        if (i % batchSize == 0) {
          val execute = jestClient.execute(bulk.build)
          if (!execute.isSucceeded) { // 抛出异常
            throw new Exception(s"this $i times insert Batch Exception, ${execute.getJsonString}")
          }
          logger.info(s"thread name is ${Thread.currentThread().getName} this ${i / batchSize} times insert Batch success")

          bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName)
        }
      }
      if (i > 0 && i % batchSize != 0) {
        try {
          val br = jestClient.execute(bulk.build)
          if (!br.isSucceeded) {
            throw new Exception(s"last insert Batch Exception: ${br.getJsonString}")
          }
          logger.info(s"thread name is ${Thread.currentThread().getName} this ${i / batchSize + 1} times insert Batch success")
        } catch {
          case ex: Exception =>
            logger.error(s"this $i times insert Batch Exception ${ex.getMessage}")
          // throw new Exception(s"this $i times insert Batch Exception ${ex.getMessage}")
        }finally {
          jestClient.close()
        }
        logger.info(s"thread name is ${Thread.currentThread().getName} insert batch success")
      }
    }
  }

  //noinspection RedundantDefaultArgument
  @throws[Exception]
  def getIndexNames(esNodesHttp: String, indexNameAliases: String): List[String] = {
    val url = s"http://$esNodesHttp/$indexNameAliases/_alias"
    val response = HttpUtil.get(url, "")
    logger.info(s"getIndexName response: $response")
    val r = regJson(response)
    val list = JavaConversions.mapAsScalaMap(r)
          .map { r =>
            val aliases = r._2.asInstanceOf[Map[String, Any]].head._2.asInstanceOf[Map[String, Any]].keySet.head
            if (aliases == indexNameAliases) r._1 else ""
          }.filter(_.nonEmpty)
          .toList
    if (list.isEmpty){
      throw new Exception(s"indexName(Aliases) $indexNameAliases, no exist indexName")
    }else {
      list
    }
  }

  def deleteAliases(esNodesHttp: String, indexName: String, indexNameAliases: String): Unit ={
    val url = s"http://$esNodesHttp/_aliases"
    val json = "{\"actions\": [{\"remove\": {\"index\": \"" + indexName + "\", \"alias\": \"" + indexNameAliases+ "\"}}]}"
    HttpUtil.post(url, json)
    logger.info(s"delete $indexName aliases success")
  }

  def deleteData(esNodesHttp: String, indexName: String): Unit ={
    val url = s"http://$esNodesHttp/$indexName/_delete_by_query"
    val json = "{\"query\": {\"match_all\": {}}}"
    HttpUtil.post(url, json)
    logger.info(s"delete $indexName data success")
  }


  def deleteIndexName(esNodesHttp: String, indexName: String): Unit ={
    val url = s"http://$esNodesHttp/$indexName/"
    HttpUtil.delete(url)
    logger.info(s"delete $indexName success")
  }

  def getTotal(esNodesHttp: String, indexName: String): Long ={
    val url = s"http://$esNodesHttp/$indexName/_count"
    val map = regJson(HttpUtil.get(url))
     map.get("count").toString.toDouble.toLong
  }

  def getEsNodes(esNodesHttp: String): Array[String] ={
    val url = s"http://$esNodesHttp/_cat/nodes"
     HttpUtil.get(url)
  }

  def regJson(json: String): JSONObject = {
    try {
       JSON.parseObject(json)
    }catch {
        case _: Exception =>
          new JSONObject()
      }
  }
}
