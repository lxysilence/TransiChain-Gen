package TripChain.pre
import common.CommonScalaMethod
import org.apache.spark.SparkContext
import utils.SystemUtils

import java.text.DecimalFormat

object take {

  var mapPath = "/root/lxying/map/overpass.bfmap"
  var dataPathPrefix = "/user/root/lxying/data/2018017/"
  var indexPrefix = "/user/root/lxying/data/2018017/"
  var resultPathPrefix = "/user/root/lxying/resultData/pro/"
  var regionDataPath = "/user/root/lxying/sz_points_v3.csv"

  if (SystemUtils.isWindows) {
    dataPathPrefix = "D:\\traffic\\TripChain\\experientData\\ExperimentData\\our\\adaptive\\part*"
    indexPrefix = "D:\\traffic\\TripChain\\experientData\\region_index\\part*"
    resultPathPrefix = "D:\\traffic\\TripChain\\experientData"
    regionDataPath = "output/sz_points_v3.csv"
  }

  def main(args: Array[String]): Unit = {
    val sc = CommonScalaMethod.buildSparkContext("TrajectoryLengthCount")

    try {
      // 读取数据文件
      val data = sc.textFile(dataPathPrefix)

      // 计算每条记录的轨迹长度（逗号分隔的字段数+1）
      val trajectoryLengths = data.map { line =>
        // 轨迹长度 = 逗号数量 + 1
        val length = line.count(_ == ',') + 1
        (length, 1)
      }

      // 按轨迹长度分组统计
      val lengthCounts = trajectoryLengths.reduceByKey(_ + _)
        .sortBy(_._1, ascending = true)

      // 打印统计结果（开发调试用）
      lengthCounts.collect().foreach { case (length, count) =>
        println(s"轨迹长度: $length, 数量: $count")
      }

      // 保存结果到HDFS/本地文件系统
      val resultPath = resultPathPrefix + "/trajectory_length_counts"
      lengthCounts.saveAsTextFile(resultPath)
      println(s"结果已保存到: $resultPath")

    } finally {
      sc.stop()
    }
  }


//  def main(args: Array[String]): Unit = {
//    val sc = CommonScalaMethod.buildSparkContext("DataFrame_APP")
//
//    // 1. 读取原始数据
//    val originalRDD = sc.textFile(dataPathPrefix + "*")
//
//    // 2. 提取前1,000,000条数据
//    val sampledRDD = if (originalRDD.count() >= 1000000) {
//      // 如果数据量>=100万，直接取前100万条
//      sc.parallelize(originalRDD.take(750000))
//    } else {
//      // 如果数据量不足100万，输出警告并返回全部数据
//      println(s"Warning: Only ${originalRDD.count()} records available, less than 1,000,000")
//      originalRDD
//    }
//
//    // 3. 保存结果
//    sampledRDD.coalesce(1).saveAsTextFile(resultPathPrefix + "/sampled_data")
//
//    println(s"Successfully saved ${sampledRDD.count()} records")
//    sc.stop()
//  }


//  def main(args: Array[String]): Unit = {
//    val sc = CommonScalaMethod.buildSparkContext("DataFrame_APP")
//
//    // 1. 读取原始数据
//    val originalRDD = sc.textFile(dataPathPrefix)
//
//    // 2. 将分号替换为空格
//    val transformedRDD = originalRDD.map { line =>
//      // 将每行中的所有分号替换为空格
//      line.replace(';', ' ')
//    }
//
//    // 3. 保存结果
//    transformedRDD.coalesce(1).saveAsTextFile(resultPathPrefix)
//
//    println("数据转换完成：所有分号已替换为空格")
//    sc.stop()
//  }


//  def main(args: Array[String]): Unit = {
//    val sc = CommonScalaMethod.buildSparkContext("DataFrame_APP")
//
//    // 1. 读取原始数据
//    val originalRDD = sc.textFile(dataPathPrefix)
//
//    // 2. 提取所有经纬度并格式化
//    val coordinatesRDD = originalRDD.flatMap { line =>
//      // 按逗号分割字段
//      val fields = line.split(",")
//
//      // 提取每个字段的经纬度
//      fields.flatMap { field =>
//        // 按分号分割每个字段的组成部分
//        val parts = field.split(" ")
//        if (parts.length >= 3) {
//          // 提取经纬度（索引2和3）
//          val longitude = parts(1).trim
//          val latitude = parts(2).trim
//
//          // 保留三位小数
//          Some((formatDecimal(longitude), formatDecimal(latitude)))
//        } else {
//          None
//        }
//      }
//    }.distinct() // 去重
//
//    // 3. 排序（先按经度排序，再按纬度排序）
//    val sortedRDD = coordinatesRDD.map { case (lon, lat) =>
//        (lon.toDouble, lat.toDouble) // 转换为Double便于数值排序
//      }.sortBy(coord => coord)
//      .map { case (lon, lat) =>
//        s"${formatDecimal(lon.toString)} ${formatDecimal(lat.toString)}" // 转换回格式化字符串
//      }
//
//    // 4. 保存结果
//    sortedRDD.coalesce(1).saveAsTextFile(resultPathPrefix)
//
//    println("经纬度提取、格式化和排序完成")
//    sc.stop()
//  }
//
//  // 格式化小数，保留三位小数
//  private def formatDecimal(value: String): String = {
//    if (value.isEmpty) return ""
//
//    try {
//      val decimalFormat = new DecimalFormat("0.000")
//      decimalFormat.format(value.toDouble)
//    } catch {
//      case e: NumberFormatException =>
//        println(s"无法解析数值: $value")
//        value
//    }
//  }


//  def main(args: Array[String]): Unit = {
//    val sc = CommonScalaMethod.buildSparkContext("DataFrame_APP")
//
//    // 1. 读取index文件并创建映射（经纬度 -> 行号）
//    val indexMap = sc.textFile(indexPrefix)
//      .zipWithIndex() // 添加行号索引
//      .map { case (line, index) =>
//        // 提取经纬度作为键
//        val coords = line.trim.split("\\s+")
//        if (coords.length >= 2) {
//          val key = s"${coords(0)} ${coords(1)}" // 经度+空格+纬度
//          (key, index + 1) // 行号从1开始
//        } else {
//          ("", 0L) // 无效行
//        }
//      }
//      .filter(_._1.nonEmpty) // 过滤无效行
//      .collect() // 收集到Driver
//      .toMap
//
//    // 2. 广播映射到所有Worker节点
//    val indexBroadcast = sc.broadcast(indexMap)
//
//    // 3. 读取数据文件并替换经纬度为编号
//    val dataRDD = sc.textFile(dataPathPrefix)
//      .map { line =>
//        // 分割字段
//        val fields = line.split(",")
//
//        // 处理每个字段
//        val processedFields = fields.map { field =>
//          // 分割字段内容
//          val parts = field.trim.split("\\s+")
//          if (parts.length >= 3) {
//            // 提取时间戳
//            val timestamp = parts(0)
//            // 组合经纬度键
//            val coordKey = s"${parts(1)} ${parts(2)}"
//
//            // 查找编号
//            indexBroadcast.value.get(coordKey) match {
//              case Some(index) => s"$timestamp $index" // 替换为编号
//              case None => field // 未找到编号，保留原字段
//            }
//          } else {
//            field // 格式错误，保留原字段
//          }
//        }
//
//        // 重新组合字段
//        processedFields.mkString(",")
//      }
//
//    // 4. 保存结果
//    dataRDD.coalesce(1).saveAsTextFile(resultPathPrefix)
//
//    println("经纬度替换为编号完成")
//    sc.stop()
//  }

}