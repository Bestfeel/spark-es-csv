package com.giziwts.util

import java.text.{MessageFormat, SimpleDateFormat}

import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeUtils}
import org.json4s.JsonAST._
import org.json4s.{JNothing => _, JNull => _, JValue => _}

import scala.collection.immutable
import scala.util.Try

/**
  * Created by feel on 16/3/30.
  */
class ConfigContext(config: Config) {
  config.checkValid(ConfigFactory.defaultReference(), "util")

  def this() {
    this(ConfigFactory.load("application.conf"))
  }

  def get(path: String) = config.getString(path)
}

object Util {


  def getConfig(path: String) = new ConfigContext().get(path)

  val DATEFORMAT_V1 = "yyyy-MM-dd HH:mm:ss.SSS"
  val DATEFORMAT_V2 = "yyyy-MM-dd"
  val DATEFORMAT_V3 = "{0}=yyyy/{1}=MM/{2}=dd"

  def dateFormat(dateFormat: String) = new SimpleDateFormat(dateFormat)


  val DEFAULT_TIMESTAMP = (DateTimeUtils.currentTimeMillis() - 24 * 3600 * 1000) / 1000L


  implicit class ParseJson2Map(json: String) {
    def toMaps = JsonUtil.toMap(json)
  }

  implicit class ParseMap2Json(map: immutable.Map[String, JValue]) {

    def toJson(fieldName: String, addField: String) = JsonUtil.map2Json(map, fieldName, addField)

  }

  implicit class Parsecsv2Json(json: String) {
    def csv2Json(colume: String, contentSplit: String, timeFieldName: String, addField: String)
    = JsonUtil.csv2Json(json, colume, contentSplit, timeFieldName, addField)
  }

  /**
    * 根据时间字符串格式化 对应的年 月 日
    * parseYearMonthDay(2016-03-03)=(2016,03,03)
    *
    * @param dateStr
    * @return
    */
  def parseYearMonthDay(dateStr: String) = {

    val dateTime: DateTime = DateTimeFormat.forPattern(DATEFORMAT_V2).parseDateTime(dateStr)

    (dateTime.year().get(), dateTime.monthOfYear().get(), dateTime.dayOfMonth().get())

  }


  /**
    * 检查输出目录是否存在,如果存在就删除
    *
    * @param hdfsFilePath
    * @return
    */
  def delHdfsFiles(hdfsFilePath: String): AnyVal = {

    val hdfs = FileSystem.get(new Configuration())

    // 小心别把 /  给删除了
    if (hdfs.exists(new Path(hdfsFilePath)) && !hdfsFilePath.equals("/")) hdfs.delete(new Path(hdfsFilePath), true)
  }

  /**
    * 过滤 集合内容
    *
    * @param fields
    * @param field
    * @return
    */
  def filterString(fields: Seq[String], field: String): Boolean = fields.contains(field)

  /**
    * 时间字符串转换为ts  Long, 如果时间处理异常则为 系统当前时间的前一天
    *
    * @param dateStr
    * @return
    */
  def dateFormat2Long(dateStr: String): Long = Try {

    dateFormat(DATEFORMAT_V1).parse(dateStr).getTime / 1000L

  }.getOrElse(DEFAULT_TIMESTAMP)

  /**
    * 主要是将 JDouble  转换为long  把时间 ts 的小数位给去除
    *
    * @param json
    * @return
    */
  def jv2String(json: JValue): String = {
    import org.json4s._
    implicit lazy val formats = org.json4s.DefaultFormats
    json match {
      case JBool(value) => value.toString
      case JDecimal(value) => value.toString
      case JInt(value) => value.toString
      case JDouble(value) => value.toLong.toString
      case JString(value) => value.toString
      case JNull => ""
      case JNothing => ""
      case jv: JValue => jv.extract[String]
    }
  }

  /**
    *
    * @param dateStr "2016-02-01"
    * @return year=2016/month=02/day=01
    */
  def getYearMonthDayFormat(dateStr: String): String = {
    val dateTime = DateTimeFormat.forPattern(DATEFORMAT_V2).parseDateTime(dateStr)
    MessageFormat.format(dateTime.toString(DATEFORMAT_V3), "year", "month", "day")
  }

  /**
    * 正则匹配路径是否存在
    *
    * @param sourcePath
    * @param regex
    * @return
    */
  def booleanPath(sourcePath: String, regex: String): Boolean = {
    val hdfs = FileSystem.get(new Configuration())
    val statu = hdfs.globStatus(new Path(sourcePath), new PathFilter {
      override def accept(path: Path): Boolean = !path.toString().matches(regex)

    })
    statu != null && statu.length > 0
  }

  def booleanPath(sourcePath: String): Boolean = {
    val hdfs = FileSystem.get(new Configuration())

    hdfs.exists(new Path(sourcePath))
  }

  //
}
