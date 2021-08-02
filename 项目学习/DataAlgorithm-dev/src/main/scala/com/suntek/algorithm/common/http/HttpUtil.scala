package com.suntek.algorithm.common.http

import com.alibaba.fastjson.JSON
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods._
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.http.{Consts, HttpStatus, NameValuePair}
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStreamReader
import java.net.URL
import java.util
import java.util.Properties
import scala.collection.mutable
import scala.util.parsing.json.JSON.parseFull
import scala.collection.JavaConversions.propertiesAsScalaMap


object HttpUtil {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val httpClient: CloseableHttpClient = HttpClients.createDefault()

  val ENCODE = "UTF-8"
  val BATCH = 200
  val HTTP_CONNECT_TIME_OUT: Int = 100000000
  val HTTP_CONNECTION_REQUEST_TIME_OUT: Int = 100000000
  val HTTP_SOCKET_TIME_OUT: Int = 100000000

  def sendGet(url: String): mutable.Map[String, String] = {
    val map =  mutable.Map[String, String]()
    try{
      logger.info(s"url: ${url}")
      val httpget = new HttpGet(url)
      val response = httpClient.execute(httpget)
      val content = response.getEntity.getContent
      val properties = new Properties()
      val istr = new InputStreamReader(content, "UTF-8")
      properties.load(istr)
      properties.foreach(p => {
        map += (p._1 -> p._2)
      } )
      istr.close()
    }catch {
      case ex: Exception => logger.error(s"请求${url}异常：",ex)
    }
    map
  }

  def sendPost(url: String,
               map: mutable.Map[String, String]
              ): String = {
    val formparams = new util.ArrayList[NameValuePair]()
    map.foreach(m =>{
      formparams.add(new BasicNameValuePair(m._1, m._2))
    })
    val entity = new UrlEncodedFormEntity(formparams, Consts.UTF_8)
    val httppost = new HttpPost(url)
    httppost.setEntity(entity)
    val response = httpClient.execute(httppost)
    logger.info(response.toString)
    val result = EntityUtils.toString(response.getEntity)
    logger.info(result)
    result
  }

  def sendPost(url: String,
               requestJson: String
              ): String = {
    val entity: StringEntity = new StringEntity(requestJson, ContentType.APPLICATION_JSON)
    val httppost = new HttpPost(url)
    httppost.setEntity(entity)
    val response = httpClient.execute(httppost)
    val result = EntityUtils.toString(response.getEntity)
    println(result)
    logger.info(result)
    result
  }


  /*********************基本的 POST PUT GET DELETE 请求 ***************************************************/
  /**
   *  POST 请求
   *  SC_CREATED = 201
   * @param json 参数信息 JSON
   * @return response
   */
  def post(postUrl: String, json: String): String = {
    val httpPost = new HttpPost(postUrl)
    httpClientBase(json, httpPost)
  }

  /**
   *  PUT 请求
   *  SC_OK = 200
   * @param putUrl http url
   * @param json 参数信息 JSON
   * @return response
   */
  def put(putUrl:String, json: String):String = {
    val httpPut = new HttpPut(putUrl)
    httpClientBase(json, httpPut)
  }

  /**
   *  GET 请求
   * @param json   参数信息
   * @return response
   */
  def get(getUrl: String, json: String = ""): String = {

    val httpClient = createCloseableHttpClient
    val requestConfig = initHttpClient
    var resultString = ""
    /**
     * 内部函数, 执行 httpClient.execute(httpEntity)
     */
    def execute(): CloseableHttpResponse = {
      // 创建uri 
      val builder = new URIBuilder(getUrl)
      regJson(json).foreach(each => builder.addParameter(each._1, each._2.toString))
      val uri = builder.build()
      // 创建http GET请求 
      val httpGet = new HttpGet(uri)
      //给这个请求设置请求配置
      httpGet.setConfig(requestConfig)
      httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2")
      // 执行请求 
      val response = httpClient.execute(httpGet)
      resultString = EntityUtils.toString(response.getEntity, "UTF-8")
      response
    }
    catchException(() => execute(), httpClient)
    resultString = resultString.replaceAll("""\\""","")
    logger.info(s"GET REQUEST result: $resultString")
    //判定是否包含异常信息
    if (JSON.parseObject(resultString).get("exception") != null  ||
      JSON.parseObject(resultString).get("exceptions") != null ){
      throw new Exception(resultString)
    }
    resultString
  }

  /**
   *  GET 请求
   * @param 返回es的节点数
   * @return response
   */
  def get(getUrl: String): Array[String] = {

    val httpClient = createCloseableHttpClient
    val requestConfig = initHttpClient
    var resultString = ""
    /**
     * 内部函数, 执行 httpClient.execute(httpEntity)
     */
    def execute(): CloseableHttpResponse = {
      // 创建uri 
      val builder = new URIBuilder(getUrl)
      val uri = builder.build()
      // 创建http GET请求 
      val httpGet = new HttpGet(uri)
      //给这个请求设置请求配置
      httpGet.setConfig(requestConfig)
      httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2")
      // 执行请求 
      val response = httpClient.execute(httpGet)
      resultString = EntityUtils.toString(response.getEntity, "UTF-8")
      response
    }
    catchException(() => execute(), httpClient)
    resultString = resultString.replaceAll("""\\""","")
    logger.info(s"GET REQUEST result: $resultString")
    resultString.trim.split("\\n").map(_.split("\\s").head)
  }

  /**
   *  DELETE 请求
   * @param deleteUrl http url
   * @return response
   */
  def delete(deleteUrl: String): String = {
    val httpClient =  createCloseableHttpClient
    val requestConfig = initHttpClient
    var resultString = ""
    /**
     * 内部函数, 执行 httpClient.execute(httpEntity)
     */
    def execute(): CloseableHttpResponse ={
      val url = new URL(deleteUrl)
      val httpDelete = new HttpDelete(url.toString)
      httpDelete.setConfig(requestConfig)
      // 执行http请求 
      val response = httpClient.execute(httpDelete)
      if (response.getStatusLine.getStatusCode != HttpStatus.SC_NO_CONTENT) {
        resultString = EntityUtils.toString(response.getEntity, "utf-8")
      }else {
        resultString = "{\"response\":\"delete success\"}"
      }
      response
    }
    catchException(() => execute(), httpClient)
    resultString
  }
  /**
   *  创建默认的 client
   * 创建Httpclient对象
   * @return CloseableHttpClient
   */
  def createCloseableHttpClient: CloseableHttpClient = HttpClients.createDefault()

  /**
   * 初始化 HttpClient
   * @return RequestConfig
   */
  def initHttpClient: RequestConfig = {
    //设置请求超时时间（各项超时参数具体含义链接）
    RequestConfig.custom()
      .setConnectTimeout(HTTP_CONNECT_TIME_OUT)
      .setConnectionRequestTimeout(HTTP_CONNECTION_REQUEST_TIME_OUT)
      .setSocketTimeout(HTTP_SOCKET_TIME_OUT)
      .build()
  }

  /**
   *  关闭 response  httpClient
   * @param response CloseableHttpResponse
   * @param httpClient CloseableHttpClient
   */
  def close(response: CloseableHttpResponse, httpClient: CloseableHttpClient): Unit = {
    try {
      response.close()
      httpClient.close()
    } catch {
      case ex: Exception => throw ex
    }
  }

  /**
   *  json 转换成 Map
   * @param json json
   * @return
   */

  def regJson(json: String): Map[String, Any] =
    parseFull(json) match {
      case Some(map: Map[String, Any] @unchecked) => map
      case _ => Map[String, Any]()
    }

  /**
   * 对执行过程进行异常捕获
   * @param func  execute
   * @param httpClient httpClient
   */
  def catchException(func: () => CloseableHttpResponse, httpClient: CloseableHttpClient): Unit ={
    var response: CloseableHttpResponse = null
    try {
      response = func.apply()
    } catch {
      case ex: Exception => throw  ex
    } finally {
      close(response, httpClient)
    }
  }
  /**
   *
   * @param json json 协议
   * @param httpEntity http请求方式
   * @return 请求信息
   */
  def httpClientBase(json: String, httpEntity: HttpEntityEnclosingRequestBase): String = {
    val httpClient = createCloseableHttpClient
    val requestConfig = initHttpClient
    var resultString = ""

    /**
     * 内部函数, 执行 httpClient.execute(httpEntity)
     */
    def execute(): CloseableHttpResponse ={
      httpEntity.setConfig(requestConfig)
      // 创建请求内容 ，发送json数据需要设置contentType
      val entity = new StringEntity(json, ContentType.APPLICATION_JSON)
      httpEntity.setEntity(entity)
      // 执行http请求 
      val response = httpClient.execute(httpEntity)
      resultString = EntityUtils.toString(response.getEntity, "utf-8")
      response
    }
    catchException(() => execute(), httpClient)
    resultString
  }

}
