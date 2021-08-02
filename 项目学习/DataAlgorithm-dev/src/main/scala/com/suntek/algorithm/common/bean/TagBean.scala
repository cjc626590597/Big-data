package com.suntek.algorithm.common.bean

@SerialVersionUID(2L)
class TagBean (t_databaseType: String = "",
               t_archiveType: Int = 2,
               t_jdbcUrl: String = "",
               t_username: String = "",
               t_password: String = "",
               t_archiveName: String = "",
               t_archiveIdName: String = "",
               t_modelId: Int,
               t_pkId: String = "",
               t_residName: String = "",
               t_sceneId: Int ,
               t_tagColName: String = "",
               t_tagId: String ,
               t_tagName: String = "",
               t_updateType: Int = 2
              ) extends Serializable {

  var databaseType: String = t_databaseType
  var archiveType: Int = t_archiveType
  var jdbcUrl: String = t_jdbcUrl
  var userName: String = t_username
  var password: String = t_password
  var archiveName: String = t_archiveName
  var archiveIdName: String = t_archiveIdName
  var modelId: Int = t_modelId
  var pkId: String = t_pkId
  var residName: String = t_residName
  var sceneId: Int = t_sceneId
  var tagColName: String = t_tagColName
  var tagId: String = t_tagId
  var tagName: String = t_tagName
  var updateType: Int = t_updateType


  override def toString: String = {
    s"$databaseType,$archiveType,$jdbcUrl,$userName,$password,$archiveName,$archiveIdName,$pkId,$residName,$sceneId,$tagColName,$tagId,$tagName,$updateType"
  }

}
