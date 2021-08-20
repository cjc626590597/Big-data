package com.suntek.algorithm.process.distribution

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhy
  * @date 2020-12-4 13:08
  */
class DeviceGroup(indext: String) {
  var index = indext
  val deviceList1 = new ArrayBuffer[String]()
  val deviceList2 = new ArrayBuffer[String]()

  def addDevicePari(device1: String, device2: String): Unit ={
    deviceList1.append(device1)
    deviceList2.append(device2)
  }

  def findIndex(device1: String, device2: String): Int = {
    if(deviceList1.indexOf(device1) != -1 && deviceList2.indexOf(device2) != -1){
      return 3
    }
    if(deviceList1.indexOf(device1) != -1){
      return 1
    }
    if(deviceList2.indexOf(device2) != -1){
      return 2
    }
    return -1
  }
}
