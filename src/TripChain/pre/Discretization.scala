package TripChain.pre
//将时间按5min，15min，30min，密度自适应离散化
import common.CommonScalaMethod
import experiment.others.GeneralFunctionSets
import org.apache.spark.SparkContext
import utils.SystemUtils

object Discretization {

  // 路径配置保持不变...
  var mapPath = "/root/lxying/map/overpass.bfmap"
  var dataPathPrefix = "/user/root/lxying/data/2018017/"
  var resultPathPrefix = "/user/root/lxying/resultData/pro/"
  var regionDataPath = "/user/root/lxying/sz_points_v3.csv"

  if (SystemUtils.isWindows) {
    dataPathPrefix = "D:\\traffic\\TripChain\\experientData\\time\\10minflow"
    resultPathPrefix = "D:\\traffic\\TripChain\\experientData\\time\\station10minflow"
    regionDataPath = "output/sz_points_v3.csv"
  }

  def main(args: Array[String]): Unit = {
    val sc = CommonScalaMethod.buildSparkContext("Discretization_APP")
        //将按流量密度自适应划分好的时间戳映射到原始数据
        // 1. 读取索引文件并构建广播变量
//     读取并处理index文件
        val indexRDD = sc.textFile("D:\\traffic\\TripChain\\experientData\\time\\adaptive\\index") // 替换为实际路径
        val tsMap = indexRDD.flatMap { line =>
          val parts = line.split(",")
          val rangePart = parts(0)
          val newTs = parts(1).toInt
          rangePart.split("-") match {
            case Array(start) => Seq((start.toInt, newTs))
            case Array(start, end) => (start.toInt to end.toInt).map(ts => (ts, newTs))
          }
        }.collect().toMap

        // 广播时间戳映射表
        val broadcastTsMap = sc.broadcast(tsMap)

        // 读取并处理数据文件
        val dataRDD = sc.textFile("D:\\traffic\\TripChain\\experientData\\temp_TripChain\\10min_TripChain\\part*") // 替换为实际路径
        val result = dataRDD.map { line =>
            val parts = line.split(",")
            (parts(0), parts(1).toInt, parts(2).toInt)
          }.map { case (station, oldTs, passengers) =>
            ((station, broadcastTsMap.value(oldTs)), passengers)
          }.reduceByKey(_ + _)
          // 新增排序逻辑：先按站点排序，再按新时间戳排序
          .map { case ((station, newTs), total) => (station, newTs, total) }
          .sortBy(x => (x._1, x._2)) // 使用复合排序键
          .map { case (station, newTs, total) =>
            s"$station,$newTs,$total"
          }


    //        val rdd = sc.textFile(dataPathPrefix + "/part*")
    //        //时间自适应离散化，先按5min转换为时间戳，再通过合并获得最终划分方式
    //        //将数据以，为分割展平
    //        val flatrdd = rdd.flatMap { line =>
    //          // 1. 按逗号分割成多个独立记录
    //          val records = line.split(",")
    //
    //          // 2. 过滤空值并返回迭代器
    //          records.toIterator
    //        }
    //
    //
    //
    //
    //        // 统计每个站点时序客流量
    //        val result = flatrdd
    //          .map(line => {
    //            val parts = line.split(";")
    //            ((parts(1), parts(0)), 1) // ((站点,时间戳), 1)
    //          })
    //          .reduceByKey(_ + _)         // 聚合计数
    //          .map {
    //            case ((station, ts), count) =>
    //              (station, (ts.toInt, ts, count)) // (站点, (时间戳数值, 原始时间戳, 计数))
    //          }
    //          .groupByKey()               // 按站点分组
    //          .flatMap { case (station, data) =>
    //            // 按数值时间戳排序，保持原始时间戳格式
    //            data.toList
    //              .sortBy(_._1)          // 使用数值时间戳排序
    //              .map(t => (station, t._2, t._3))
    //          }
    //          .map { case (station, ts, count) =>
    //            s"$station,$ts,$count"   // 格式化输出
    //          }

    //        val rdd = sc.textFile(dataPathPrefix + "/part*")
    //        val data = rdd.map { line =>
    //          val parts = line.split(",")
    //          (parts(0), parts(1).toInt, parts(2).toInt)
    //        }
    //
    //        // 获取所有唯一的站点
    //        val stationsRDD = data.map(_._1).distinct()
    //
    //        // 生成每个站点的完整时间序列[0-96]
    //        val fullTimeRDD = stationsRDD.flatMap { site =>
    //          (0 to 96).map(time => (site, time))
    //        }
    //
    //        // 将原始数据转换为键值对((站点, 时间), 计数值)
    //        val originalKeyValue = data.map { case (site, time, count) =>
    //          ((site, time), count)
    //        }
    //
    //        // 左外连接并填充缺失值
    //        val resultRDD = fullTimeRDD.map { case (site, time) => ((site, time), ()) }
    //          .leftOuterJoin(originalKeyValue)
    //          .map { case ((site, time), (_, countOpt)) =>
    //            // 保持时间字段为Int类型用于后续排序
    //            (site, time, countOpt.getOrElse(0))
    //          }
    //
    //        // 添加排序逻辑：先按站点字典序，再按时间数值序
    //        val sortedRDD = resultRDD.sortBy { case (site, time, _) =>
    //          (site, time)
    //        }
    //
    //        // 按原格式输出并保存（时间字段此时转为字符串）
    //        val result = sortedRDD.map { case (site, time, count) =>
    //          s"$site,$time,$count"
    //        }
    //
        result.coalesce(1).saveAsTextFile(resultPathPrefix)
        sc.stop()

      }
    }


//    var rdd = sc.textFile(dataPathPrefix + "/part*")
//
//    // 提取带时间戳的出行链
//    val proData = extract(sc, rdd)
//
//    // 只保留6：00-22：00的数据
//    val filteredData = proData.filter(line => {
//      val columns = line.split(",").map(_.split(";"))
//      columns.length match {
//        case 2 =>
//          columns(0)(0).toInt >= 72 && columns(0)(0).toInt < 264 &&
//            columns(1)(0).toInt >= 72 && columns(1)(0).toInt < 264
//        case 4 =>
//          columns(0)(0).toInt >= 72 && columns(0)(0).toInt < 264 &&
//            columns(1)(0).toInt >= 72 && columns(1)(0).toInt < 264 &&
//            columns(2)(0).toInt >= 72 && columns(2)(0).toInt < 264 &&
//            columns(3)(0).toInt >= 72 && columns(3)(0).toInt < 264
//        case 6 =>
//          columns(0)(0).toInt >= 72 && columns(0)(0).toInt < 264 &&
//            columns(1)(0).toInt >= 72 && columns(1)(0).toInt < 264 &&
//            columns(2)(0).toInt >= 72 && columns(2)(0).toInt < 264 &&
//            columns(3)(0).toInt >= 72 && columns(3)(0).toInt < 264 &&
//            columns(4)(0).toInt >= 72 && columns(4)(0).toInt < 264 &&
//            columns(5)(0).toInt >= 72 && columns(5)(0).toInt < 264
//        case _ => false
//      }
//    })
//
//    val timeData = filteredData.map(line => {
//      val columns = line.split(",").map(_.split(";"))
//      // 时间转换为时间戳
//      columns.foreach(col => col(0) = (col(0).toInt - 36).toString)
//      columns.map(_.mkString(";")).mkString(",")
//    })
//
//    // 将每个站点数据按时间戳排序
//    val sortedData = timeData
//      .map(line => {
//        val parts = line.split(",")
//        val firstPoint = parts(0).split(";")
//        val site = firstPoint(1) // 使用第一个点的站点作为分组依据
//        (site, line)
//      })
//      .groupByKey() // 按站点分组
//      .flatMapValues(lines => lines.toList.sortBy(_.split(",")(0).split(";")(0).toInt)) // 组内按时间戳排序
//      .map(_._2) // 只保留排序后的原始数据
//
//    // 添加字段处理逻辑
//    val processedData = sortedData.map { line =>
//      val fields = line.split(",")
//      fields.length match {
//        case 2 =>
//          // 保留全部字段
//          fields.mkString(",")
//        case 4 =>
//          // 删除第二个字段
//          Array(fields(0), fields(2), fields(3)).mkString(",")
//        case 6 =>
//          // 删除第二和第四个字段
//          Array(fields(0), fields(2), fields(4), fields(5)).mkString(",")
//        case _ =>
//          // 其他情况保留原样
//          line
//      }
//    }
//
//    processedData.coalesce(1).saveAsTextFile(resultPathPrefix)
//    sc.stop()
//  }
//
//  // 将TripChain中时间转换为时间戳,根据需要切换对应分钟的时间戳函数
//  def extract(sc: SparkContext, data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
//    data.map(line => {
//      val columns = line.split(",").map(_.split(";"))
//
//      // 时间转换为时间戳
//      columns.foreach(col => {
//        col(0) = GeneralFunctionSets.minutesOfDayV5(col(0)).toString
//      })
//
//      columns.map(_.mkString(";")).mkString(",")
//    })
//  }
//}







