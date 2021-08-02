package com.suntek.algorithm.common.util

import java.sql.{Connection, SQLException}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-18 17:58
  * Description: 替换spark的jdbc写库方式，针对一些特殊需求，比如snowball字符串写入会多引号等问题
  */
object SnowBallSaveUtil {
  val logger = LoggerFactory.getLogger(this.getClass)


  /**
    * Saves the RDD to the database in a single transaction.
    */
  def saveTable(
                 df: DataFrame,
                 getConnection: () => Connection,
                 options: JdbcOptionsInWrite): Unit = {

    val url = options.url
    val table = options.table
    val rddSchema = df.schema

    val batchSize = options.batchSize

    val valueList = for(_ <- rddSchema.fields.toList) yield "?"
    val insertSql = s"insert into $table( ${rddSchema.fieldNames.mkString(",")}) values (${valueList.mkString(",")})"
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition(iterator => savePartition(
      getConnection, table, iterator, rddSchema, insertSql, batchSize, options)
    )
  }

  /**
    * Saves a partition of a DataFrame to the JDBC database.  This is done in
    * a single database transaction (unless isolation level is "NONE")
    * in order to avoid repeatedly inserting data as much as possible.
    *
    * It is still theoretically possible for rows in a DataFrame to be
    * inserted into the database more than once if a stage somehow fails after
    * the commit occurs but before the stage can return successfully.
    *
    * This is not a closure inside saveTable() because apparently cosmetic
    * implementation changes elsewhere might easily render such a closure
    * non-Serializable.  Instead, we explicitly close over all variables that
    * are used.
    */
  def savePartition(
                     getConnection: () => Connection,
                     table: String,
                     iterator: Iterator[Row],
                     rddSchema: StructType,
                     insertStmt: String,
                     batchSize: Int,
                     options: JDBCOptions): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false

    try {
      conn.setAutoCommit(false) // Everything in the same db transaction.
      val stmt = conn.prepareStatement(insertStmt)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0

        stmt.setQueryTimeout(options.queryTimeout)

        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            stmt.setObject(i + 1, row(i).asInstanceOf[Object])
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      conn.commit()
      committed = true
      Iterator.empty
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        conn.rollback()
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logger.error("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

}
