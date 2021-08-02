package com.suntek.algorithm.udf

import com.suntek.algorithm.process.util.Utils
import org.apache.hadoop.hive.ql.exec.{Description, UDF}

//@Description(
//  name = "is_valid_field",
//  value = "_FUNC_(field, category) - from the input field, category  " + "returns the value that is \" boolean \" ",
//  extended = "Example:\n" + " > SELECT _FUNC_(460xxxxxxx,\"IMEI\") FROM src;" + "mac 12，imei（15-17） ，imsi 15")
class EntityCheckUDF extends UDF {

  def evaluate(field: Object, category: String): Boolean =
    if(field==null) {
       return false
    }else {
     val  _field = field.asInstanceOf[String]
      category match {
        case "CAR" =>
          Utils.isValidHphm(_field)
        case "PHONE" =>
          Utils.isMobileNumber(_field)
        case "IMSI" =>
          Utils.isValidImsi(_field)
        case "IMEI" =>
          Utils.isValidImei(_field)
        case "MAC" =>
          Utils.isValidMac(_field)
        case _ =>
          true
      }
    }



}
