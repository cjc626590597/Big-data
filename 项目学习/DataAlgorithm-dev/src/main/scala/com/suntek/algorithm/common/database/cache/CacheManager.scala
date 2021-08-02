package com.suntek.algorithm.common.database.cache

import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties

import net.oschina.j2cache.lettuce.LettuceHashCache
import net.oschina.j2cache.{CacheProvider, J2CacheBuilder, J2CacheConfig}
import org.slf4j.{Logger, LoggerFactory}

@SerialVersionUID(2L)
class CacheManager() extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  @transient
  var file: String = s"${System.getProperty("config.location")}/j2cache.properties"
  var conf = new java.io.File(file)
  if (!conf.exists()) {
    throw new Exception(s"${file} 文件不存在")
  }
  logger.info(s"j2cache.properties: $file")
  @transient
  val properties = new Properties()
  val istr = new InputStreamReader(new FileInputStream(file), "UTF-8")

  properties.load(istr)

  istr.close()
  @transient
  val config: J2CacheConfig = J2CacheConfig.initFromConfig(properties)
  @transient
  val j2CacheBuilder: J2CacheBuilder = J2CacheBuilder.init(config)
  @transient
  val cacheProvider: CacheProvider = j2CacheBuilder.getChannel.getL2Provider
  @transient
  val cache: LettuceHashCache = cacheProvider.buildCache("DAS", null).asInstanceOf[LettuceHashCache]


  def set(region: String,
          key: String,
          value: (String, String)): Unit = {
    try{
      cache.hset(s"$region-$key", value._1, value._2.getBytes())
      logger.info(s"$region-$key")
      logger.info(new String(cache.hget(s"$region-$key", value._1)))
    }catch {
      case ex: Exception =>
        logger.error(ex.toString)
        throw new Exception(ex.toString)
    }
  }

  def removeKey(region: String, key: String): Unit = {
    try{
      cache.evict(s"$region-$key")
    }catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        throw new Exception(ex.toString)
    }

  }

  def close(): Unit={
    if(cacheProvider != null) cacheProvider.stop()
  }
}