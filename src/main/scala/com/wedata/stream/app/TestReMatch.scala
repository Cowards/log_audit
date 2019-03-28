package com.wedata.stream.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import org.apache.flink.streaming.api.scala.DataStream

import scala.io.Source
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

/**
  * @version 2019-03-29.
  * @url github.com/uk0
  * @project log_audit
  * @since JDK1.8.
  * @author firshme
  */
object TestReMatch {


  def main(args: Array[String]): Unit = {
    //定义时间格式
    val lines = Source.fromFile("/Users/zhangjianxin/Desktop/Mark/log_audit/test_log/hadoop-cmf-yarn-RESOURCEMANAGER-cdh-m2.sxkj.online.log.out").getLines.toList

    //log里面第一行的类名
    val className = "org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary"
    //若同时满足时间和类都匹配进行正则匹配和转换操作
    val filterValue = lines.filter(x => x.contains("2019-03-21") && x.contains(className))

    filterValue.foreach(line => {
      val pattern = new Regex("appId=(.+?),.*?,queue=(.+?),.*?,submitTime=(.+?),*,startTime=(.+?),*,finishTime=(.+?),*,finalStatus=(.+?),*,memorySeconds=(.+?),*,vcoreSeconds=(.+?),.*?,applicationType=(.+?),.*?", line)
      var temp_Line = "";
      println("----------------------------------------------------")
      for (m <- pattern.findAllIn(line).matchData; e <- m.subgroups) {
        temp_Line += e + "^"
      }
      println(temp_Line.substring(0,temp_Line.length-1))

    })
  }
}
