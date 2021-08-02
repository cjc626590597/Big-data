package com.suntek.algorithm.udf

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}


/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2021-2-23 16:27
  * Description:解析JSON中的关联ID，如人脸ID，人体ID等
  * example: SELECT  INFO_ID AS PERSONID,praseRelateID(@relatedType@,RELATEDLIST) AS FACEID,  JGSK FROM VIDEO_PERSON_INFO limit 1
  */

class PraseRelateIDUDTF extends GenericUDTF{


  //这个方法的作用：1.输入参数校验  2. 输出列定义，可以多于1列，相当于可以生成多行多列数据
  override def initialize(args:Array[ObjectInspector]): StructObjectInspector = {
    if (args.length != 2) {
      throw new UDFArgumentLengthException("UserDefinedUDTF takes only two argument")
    }
    if (args(0).getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("UserDefinedUDTF takes string as a parameter")
    }

    val fieldNames = new java.util.ArrayList[String]
    val fieldOIs = new java.util.ArrayList[ObjectInspector]

    //这里定义的是输出列默认字段名称
    fieldNames.add("col1")
    //这里定义的是输出列字段类型
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }


  override def process(objects: Array[AnyRef]): Unit = {

    if(objects == null || objects.length != 2 || objects(0) == null || "".equals(objects(0).toString.trim)){
        throw new UDFArgumentLengthException("PraseRelateIDUDTF must has tow arguments,eg:relatedType,filedName.")

    }

    val targetRelatedType = objects(0).toString

    if (objects(1) != null && objects(1).toString != null && !objects(1).toString.trim.equals("")) {
      val filedValue = objects(1).toString
      try {
        val jsonParent = JSON.parseObject(filedValue)
        val jsonArray = jsonParent.getJSONArray("RelatedObject")

        if (jsonArray.size() > 0) {
          for (i <- 0 until jsonArray.size) {
            val relatedType = jsonArray.getJSONObject(i).getString("RelatedType")
            if (targetRelatedType.equals(relatedType)) {
              if (jsonArray.getJSONObject(i).getString("RelatedID") != null && !"".equals(jsonArray.getJSONObject(i).getString("RelatedID"))) {
                val relatedIDs = jsonArray.getJSONObject(i).getString("RelatedID").split(";")
                forward(relatedIDs)
              }

            }
          }
        }
      }catch {
        case _: Exception =>
      }
    }
  }

  override def close(): Unit = {}
}
