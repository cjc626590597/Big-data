package com.suntek.algorithm.process.distribution

import scala.collection.mutable

/**
  * @author zhy
  * @date 2020-12-4 13:02
  */

class DeviceGroupS() {
  val deviceGroups = new mutable.HashMap[String, DeviceGroup]()

  def addDeviceGroup(index: String, d: DeviceGroup): Unit ={
    deviceGroups.put(index, d)
  }

  def find1(device: String): String = {
    val indexs = deviceGroups.keySet.toList
    var i = 0
    while (i < indexs.size){
      val key = indexs(i)
      val d = deviceGroups(key)
      if(-1 != d.deviceList1.indexOf(device)){
        return String.valueOf(key)
      }
      i += 1
    }
    return ""
  }

  def find2(device: String): String = {
    val indexs = deviceGroups.keySet.toList
    var i = 0
    while (i < indexs.size){
      val key = indexs(i)
      val d = deviceGroups(key)
      if(-1 != d.deviceList2.indexOf(device)){
        return String.valueOf(key)
      }
      i += 1
    }
    return ""
  }

  def addDevice1Device2(device1: String, device2: String): Unit = {
    if(deviceGroups.size == 0){
      val node = new DeviceGroup("0")
      node.addDevicePari(device1, device2)
      addDeviceGroup("0", node)
      return
    }
    var index = -1
    var i = 0
    val indexs = deviceGroups.keySet.toList.sortBy(v=> v.toInt)
    while (i < indexs.size && index == -1){
      val key = indexs(i)
      val d = deviceGroups(key)
      index = d.findIndex(device1, device2)
      if(index == 1){
        d.deviceList2.append(device2)
      }else if(index == 2){
        d.deviceList1.append(device1)
      }
      i += 1
    }
    if(index == -1){
      val index = indexs.last.toInt + 1
      val node = new DeviceGroup(index.toString)
      node.addDevicePari(device1, device2)
      addDeviceGroup(index.toString, node)
    }
  }

  override def toString: String = {
    deviceGroups.map(v=>{
      val key = v._1
      val value1 = v._2.deviceList1.mkString("-")
      val value2 = v._2.deviceList2.mkString("-")
      s"${key}:${value1}:${value2}"
    }).mkString("@_@")
  }
}