package TripChain.pre

import common.CommonScalaMethod
import org.apache.spark.SparkContext
import utils.SystemUtils

object pre {

  // 路径配置保持不变...
  var mapPath = "/root/lxying/map/overpass.bfmap"
  var dataPathPrefix = "/user/root/lxying/data/2018017/"
  var resultPathPrefix = "/user/root/lxying/resultData/pro/"
  var regionDataPath = "/user/root/lxying/sz_points_v3.csv"

  if (SystemUtils.isWindows) {
    dataPathPrefix = "D:\\traffic\\TripChain\\New\\experimentData\\OriginalData\\20220920"
    resultPathPrefix = "D:\\traffic\\TripChain\\experientData\\TripChain\\20220920"
    regionDataPath = "output/sz_points_v3.csv"
  }

  def main(args: Array[String]): Unit = {
    val sc = CommonScalaMethod.buildSparkContext("pre_APP")

    var rdd = sc.textFile(dataPathPrefix + "/part*")

    val RecordData = rdd.map(
      line => {
        val columns = line.split(",")
        columns(16)
      }
    )

    val FilterData = RecordData.filter(line => {
      val columns = line.split(";")
      val length = columns.length
      length == 58 || length == 39 || length == 20
    })

    val ChainData = FilterData.map(line => {
      val columns = line.split(";")
      val length = columns.length
      if (length == 20) {
        columns(0) + "," + columns(1) + ";" + columns(2) + ";" + columns(3) + ";" + columns(4) + "," + columns(11) + ";" + columns(12) + ";" + columns(13) + ";" + columns(14)
      }
      else if (length == 39) {
        columns(0) + "," + columns(1) + ";" + columns(2) + ";" + columns(3) + ";" + columns(4) + "," + columns(11) + ";" + columns(12) + ";" + columns(13) + ";" + columns(14) + "," + columns(20) + ";" + columns(21) + ";" + columns(22) + ";" + columns(23) + "," + columns(30) + ";" + columns(31) + ";" + columns(32) + ";" + columns(33)
      }
      else {
        columns(0) + "," + columns(1) + ";" + columns(2) + ";" + columns(3) + ";" + columns(4) + "," + columns(11) + ";" + columns(12) + ";" + columns(13) + ";" + columns(14) + "," + columns(20) + ";" + columns(21) + ";" + columns(22) + ";" + columns(23) + "," + columns(30) + ";" + columns(31) + ";" + columns(32) + ";" + columns(33) + "," + columns(39) + ";" + columns(40) + ";" + columns(41) + ";" + columns(42) + "," + columns(49) + ";" + columns(50) + ";" + columns(51) + ";" + columns(52)
      }
    })

    // 1. 按第一个字段去重
    val distinctRDD = ChainData.map { line =>
        val firstField = line.split(",")(0)
        (firstField, line)
      }.reduceByKey((a, b) => a) // 保留每个key的第一个记录
      .values                   // 只保留值（原始行）

    // 2. 按第一个字段排序（字符串字典序）
    val sortedRDD = distinctRDD.sortBy(
      line => line.split(",")(0), // 提取第一个字段
      ascending = true            // 升序排列
    )

    // 3. 删除每行的第一个字段
    val removedFirstFieldRDD = sortedRDD.map { line =>
      // 找到第一个逗号的位置
      val firstCommaIndex = line.indexOf(',')
      // 如果找到逗号，删除第一个字段（逗号之前的内容）
      if (firstCommaIndex >= 0) {
        line.substring(firstCommaIndex + 1)
      } else {
        // 如果没有逗号，返回空字符串或原始行（根据需求决定）
        line
      }
    }

    removedFirstFieldRDD.coalesce(1).saveAsTextFile(resultPathPrefix)
    sc.stop()
  }
}