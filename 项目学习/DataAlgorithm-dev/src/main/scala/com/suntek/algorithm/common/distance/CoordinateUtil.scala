package com.suntek.algorithm.common.distance

object CoordinateUtil {
  val EARTH_RADIUS: Double = 6378.137
  val x_pi = 3.14159265358979324 * 3000.0 / 180.0
  val pi = 3.14159265358979324
  val a = 6378245.0
  val ee = 0.00669342162296594323

  def bd09_to_gcj02(lng: Double,
                    lat: Double)
  : (Double, Double) = {
    val x = lng - 0.0065
    val y = lat - 0.006
    val z = math.sqrt(x * x + y * y) - 0.00002 * math.sin( y * x_pi)
    val theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    val gg_lng = z * math.cos(theta)
    val gg_lat = z * math.sin(theta)
    (gg_lng, gg_lat)
  }

  def transformlat(lng: Double,
                   lat: Double)
  :Double = {
    var ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(math.abs(lng))
    ret = ret + (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret = ret +  (20.0 * math.sin(lat * pi) + 40.0 * math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret = ret +  (160.0 * math.sin(lat / 12.0 * pi) + 320.0 * math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    ret
  }

  def transformlng(lng: Double,
                   lat: Double)
  :Double = {
    var ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(math.abs(lng))
    ret = ret + (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret = ret + (20.0 * math.sin(lng * pi) + 40.0 * math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret = ret + (150.0 * math.sin(lng / 12.0 * pi) + 300.0 * math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    ret
  }

  def out_of_guangzhou(lng: Double, lat: Double)
  : Boolean ={
    lng > 77.66 && lng < 135.05 && lat > 3.86 && lat < 53.55
  }

  def gcj02_to_wgs84(lng: Double,
                     lat: Double)
  : (Double, Double) = {
    var dlat = transformlat(lng - 105.0, lat - 35.0)
    var dlng = transformlng(lng - 105.0, lat - 35.0)
    val radlat = lat / 180.0 * pi
    var magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    val sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi )
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi )
    val mglat = lat + dlat
    val mglng = lng + dlng
    (lng * 2 - mglng, lat * 2 - mglat)
  }

  def bd09_to_wjs84(lng: Double,
                    lat: Double)
  : (Double, Double) = {
    val lngLat =  bd09_to_gcj02(lng, lat)
    gcj02_to_wgs84(lngLat._1, lngLat._2)
  }

  def rad(d: Double): Double = {
    d * (Math.PI / 180.0)
  }

  def distance(lat1: Double,
               lng1: Double,
               lat2: Double,
               lng2: Double): Double = {
    val radLat1 = rad(lat1)
    val radLat2 = rad(lat2)
    val radLng1 = rad(lng1)
    val radLng2 = rad(lng2)
    val dlng = radLng2 - radLng1
    val dlat = radLat2 - radLat1
    val a = Math.pow(Math.sin(dlat/2), 2) +Math.cos(radLat1) * Math.cos(radLat2) *  Math.pow(Math.sin(dlng/2), 2)
    val c = 2* Math.asin(Math.sqrt(a))
    c * 6371 * 1000
  }
}
