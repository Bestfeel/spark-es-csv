package com.giziwts.util

import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.Serialization

import scala.collection.immutable
import scala.collection.immutable.HashMap

/**
  * Created by feel on 16/6/3.
  */
object JsonUtil {

  private val DATE_FORMAT: String = "yyyy-MM-dd'T'HH:mm:ssZ"
  private val DATE_FORMAT2: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  private val DATE_FORMAT3: String = "yyyy-MM-dd'T'00:00:00Z"

  implicit class MapTime(dateStr: String) {
    def formatTime = {
      //    yyyy-MM-dd
      val DateRegex1 = "^(\\d{4})-(\\d{2}||{0}\\d{1})-(\\d{2}||{0}\\d{1})$".r
      //    yyyy/MM/dd{
      val DateRegex2 = "^(\\d{4})/(\\d{2}||{0}\\d{1})/(\\d{2}||{0}\\d{1})$".r
      //    yyyyMMdd
      val DateRegex3 = "^(\\d{4})(\\d{2}||{0}\\d{1})(\\d{2}||{0}\\d{1})$".r
      // yyyy-MM-dd HH:mm:ss
      val DateRegex4 = "^(\\d{4})-(\\d{2}||{0}\\d{1})-(\\d{2}||{0}\\d{1}) (\\d{2}):(\\d{2}):(\\d{2})$".r
      // yyyy-MM-dd HH:mm:ss.SSS
      val DateRegex5 = "^(\\d{4})-(\\d{2}||{0}\\d{1})-(\\d{2}||{0}\\d{1}) (\\d{2}):(\\d{2}):(\\d{2}).(\\d{3})$".r


      dateStr match {
        case DateRegex1(year, month, day) => new DateTime(year.toInt, month.toInt, day.toInt, 0, 0).toString(DATE_FORMAT3)
        case DateRegex2(year, month, day) => new DateTime(year.toInt, month.toInt, day.toInt, 0, 0).toString(DATE_FORMAT3)
        case DateRegex3(year, month, day) => new DateTime(year.toInt, month.toInt, day.toInt, 0, 0).toString(DATE_FORMAT3)
        case DateRegex4(year, month, day, hours, minute, second) => new DateTime(year.toInt, month.toInt, day.toInt, hours.toInt, minute.toInt, second.toInt).toString(DATE_FORMAT)
        case DateRegex5(year, month, day, hours, minute, second, millis) => new DateTime(year.toInt, month.toInt, day.toInt, hours.toInt, minute.toInt, second.toInt, millis.toInt).toString(DATE_FORMAT2)
        case _ => new DateTime(dateStr.toLong * 1000L).toString(DATE_FORMAT)
      }
    }


  }


  /**
    * map 转json
    *
    * @param map
    * @param fieldName
    * @param addField
    * @return
    */
  def map2Json(map: immutable.Map[String, JValue], fieldName: String, addField: String): String = {
    import org.json4s._
    implicit val formats = DefaultFormats

    val valueMap = if (map.contains(fieldName)) {
      val ts = map.get(fieldName).get.values.toString.formatTime
      // 具体的时间逻辑处理
      map.+((addField, JString(ts).asInstanceOf[JValue]))
    } else {
      new HashMap[String, JValue]()
    }
    valueMap match {
      case value: HashMap[String, JValue] => Serialization.write(value)
      case _ => ""
    }
  }

  /**
    * 解析json  转换为 map
    *
    * @param json
    * @return
    */
  def toMap(json: String): immutable.Map[String, JValue] = {

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = org.json4s.DefaultFormats
    parse(json) match {
      case JObject(value) => value.toMap
      case _ => new HashMap[String, JValue]()
    }

  }


  /**
    * csv  转json
    *
    * @param content
    * @param colume
    * @param contentSplit
    * @return
    */
  def csv2Json(content: String, colume: String, contentSplit: String, timeFieldName: String, addField: String): String = {

    val toMap: Map[String, String] = colume.split(",").map(_.trim).zip(content.split(contentSplit).map(_.trim)).toMap

    val map = toMap.+((addField, toMap.get(timeFieldName).get.formatTime))

    implicit val formats = org.json4s.DefaultFormats

    Serialization.write(map)
  }


}
