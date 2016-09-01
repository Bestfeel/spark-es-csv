package com.giziwts.es

import java.text.MessageFormat

import com.giziwts.util.Util._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.jackson.JsonMethods._

/**
  * Created by feel on 16/6/2.
  * <pre>
  * spark  读取hive,hdfs 上的文件 并转json 写入es
  * </pre>
  *
  */
object SparkEsWrite {


  /**
    * 读取hive 表中的数据并转成json
    *
    * @param sc
    * @param dateStr
    * @param tableName
    * @param fieldname
    * @param fieldSeq
    * @param timestampField
    * @param addTimestampField
    * @param dateOption
    * @return
    */
  def sqlTable(sc: SparkContext, dateStr: String, tableName: String, fieldname: String, fieldSeq: String, timestampField: String, addTimestampField: String, dateOption: String): RDD[String] = {

    val (year, month, day) = parseYearMonthDay(dateStr)
    // 匹配sql语句
    val sqlStr = dateOption match {

      case "all" => MessageFormat.format(getConfig("jsonsql.total"), tableName, fieldname.replace(fieldSeq, "  "), dateStr, timestampField)
      case "daily" => MessageFormat.format(getConfig("jsonsql.daily"), tableName, fieldname.replace(fieldSeq, "  "), year.toString, month.toString, day.toString, dateStr, timestampField)

    }
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  

    //  计算结果输出 hdfs
    val jsonrdd: RDD[String] = if (!sqlStr.isEmpty) {
      sqlContext.sql(sqlStr).toJSON.map(json => {
        json.toMaps.toJson(timestampField, addTimestampField)
      })
    } else {
      sc.parallelize(Seq[String]())
    }
    jsonrdd

  }

  /**
    * 读取hdfs 上的文件 并转成csv
    *
    * @param sc
    * @param hdfsFile
    * @param colume
    * @param fieldSeq
    * @param timestampField
    * @param addTimestampField
    * @return
    */
  def csvFile(sc: SparkContext, hdfsFile: String, colume: String, fieldSeq: String, timestampField: String, addTimestampField: String, fileType: String): RDD[String] = {

    if (booleanPath(hdfsFile)) {
      if (fileType == "json") {
        val textFile: RDD[String] = sc.textFile(hdfsFile)
        textFile.map(parse(_)).map(line => compact(render(line)))
      } else {
        val textFile: RDD[String] = sc.textFile(hdfsFile)
        textFile.map(line => {
          line.csv2Json(colume, fieldSeq, timestampField, addTimestampField)
        })
      }
    }
    else {
      sc.parallelize(Seq[String]())

    }


  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("com.gizwits").setLevel(Level.WARN)

    // Validate args   验证11个参数是否正确
    if (args.length < 11) {
      println("Usage: [2015-05-25|dateStr] [option|[csv|table]] [tablename|hdfsPathFile]  [fieldname]  [filedSeq]  [dateOption[daily|all]]    [es [index/type]  [report:9200] [fileType[json/csv]]")
      sys.exit(1)
    }
    val (dateStr, option, tableName, fieldname, fieldSeq, timestampField, addTimestampField, dateOption, esType, esAddresss, fileType) = (
      args(0).toString,
      args(1).toString,
      args(2).toString,
      args(3).toString,
      args(4).toString,
      args(5).toString,
      args(6).toString,
      args(7).toString,
      args(8).toString,
      args(9).toString,
      args(10).toString
      )
    val sparkConf = new SparkConf()
      .setAppName("SparkEsWrite")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.default.parallelism", "16")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
    //      .setMaster("local[4]")  // 开发 调试时使用
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", esAddresss)

    val sc = new SparkContext(sparkConf)


    val jsonrdd: RDD[String] = option match {
      case "csv" => {
        csvFile(sc, tableName, fieldname, fieldSeq, timestampField, addTimestampField, fileType)
      }
      case "table" => {

        sqlTable(sc, dateStr, tableName, fieldname, fieldSeq, timestampField, addTimestampField, dateOption)

      }
      case "" => sc.parallelize(Seq[String]())
    }


    //  写入es
    EsSpark.saveJsonToEs(jsonrdd, esType)


    //end  main
  }
}
