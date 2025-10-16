//将数据格式转换成每个方法需要的格式
package TripChain.pre
import common.CommonScalaMethod
import experiment.others.GeneralFunctionSets
import org.apache.spark.SparkContext
import utils.SystemUtils

import scala.collection.mutable
object DataFrame {

  //北斗集群中数据的路径
  //由于BfmapReader读取地图文件使用的是new FileInputStream方法， 故只能使用yarn-client模式运行程序，且文件路径需绝对路径
  //如果要用yarn-cluster模式，就只能手动重写BfmapReader类再打包安装依赖
  var mapPath = "/root/lxying/map/overpass.bfmap"
  var dataPathPrefix = "/user/root/lxying/data/2018017/"
  var resultPathPrefix = "/user/root/lxying/resultData/pro/"
  var regionDataPath = "/user/root/lxying/sz_points_v3.csv"


  //中心集群中数据的路径
  //    val mapPath = "/home/frzheng/lxying_files/projects/map/map.bfmap"
  //    val dataPathPrefix = "/user/lxying/testData/taxiGps/"
  //    var resultPathPrefix = "/user/lxying/resultData/GetMaxErrorRoad/"


  //本地数据路径
  if (SystemUtils.isWindows) {
    dataPathPrefix = "D:\\traffic\\TripChain\\New\\experimentData\\ExperimentData\\MoveSim\\weekend\\"
    resultPathPrefix = "D:\\traffic\\TripChain\\experientData\\ExperimentData\\Chen"
    regionDataPath = "output/sz_points_v3.csv"
  }

  def main(args: Array[String]): Unit = {
    val sc = CommonScalaMethod.buildSparkContext("DataFrame_APP")

//    var rdd = sc.textFile(dataPathPrefix + "moveSim")



//    MoveSim需要的数据格式
//    //1.首先需要位置和经纬度的映射表gps文件，用展平的flat_Data做
//        val flatrdd = rdd.flatMap { line =>
//          // 1. 按逗号分割成多个独立记录
//          val records = line.split(",")
//
//          // 2. 过滤空值并返回迭代器
//          records.toIterator
//        }
//
//      val gps = flatrdd.map(_.split(";"))  // 直接分号切分
//      .map { cols =>
//        val longitude = cols(2).toDouble  // 第三列经度
//        val latitude = cols(3).toDouble   // 第四列纬度
////        val station = cols(1)
////        f"$station $latitude%.3f $longitude%.3f"   // 纬度在前，空格分隔
//        f"$latitude%.3f $longitude%.3f"   // 纬度在前，空格分隔
//      }
//      .distinct()  // 全局去重

//    //2.需要位置序列文件
//    // 1. 处理索引文件：创建经纬度->行号的广播映射
//    val indexMap = sc.textFile("D:\\traffic\\TripChain\\New\\experimentData\\ExperimentData\\MoveSim\\weekend\\gps")
//      .zipWithIndex() // 生成行号索引
//      .map { case (line, idx) =>
//        val formattedKey = line.split(" ") match {
//          case Array(lat, lng) =>
//            f"${lat.toDouble}%.3f ${lng.toDouble}%.3f" // 保持3位小数格式
//        }
//        (formattedKey, idx + 1) // 行号从1开始
//      }.collectAsMap()
//
//    val bcIndexMap = sc.broadcast(indexMap)
//    // 2. 处理数据文件
//    val moveSim = sc.textFile("D:\\traffic\\TripChain\\New\\experimentData\\ExperimentData\\DifferentialPrivacy\\30min\\part-00006")
//      .map(line => {
//        line.split(",")
//          .map(field => {
//            val cols = field.split(";")
//            if (cols.length >= 4) {
//              val time = cols(0)
//              val longitude = cols(2).toDouble
//              val latitude = cols(3).toDouble
//
//              // 生成匹配键
//              val geoKey = f"$latitude%.3f $longitude%.3f"
//
//              // 查找索引号
//              val index = bcIndexMap.value.getOrElse(geoKey, -1)
//
//              s"$time;$index"
//            } else {
//              field // 格式错误保持原样
//            }
//          })
//          .mkString(",")
//      })
//
//    //变成需要的MoveSim数据格式
//      val MoveSim=moveSim .flatMap { line =>
//        val fields = line.split(",")
//        val adjusted = mutable.ListBuffer.empty[(Int, String)]
//        val used = mutable.Set.empty[Int]
//        var isValid = true
//
//        // 处理每个字段并调整序号
//        for (field <- fields if isValid) {
//          field.split(";") match {
//            case Array(indexStr, value) =>
//              try {
//                val originalIndex = indexStr.toInt
//                if (originalIndex < 0 || originalIndex >= 48) {
//                  isValid = false
//                } else {
//                  var currentIndex = originalIndex
//                  // 寻找可用序号
//                  while (currentIndex < 48 && used.contains(currentIndex)) {
//                    currentIndex += 1
//                  }
//                  if (currentIndex >= 48) {
//                    isValid = false
//                  } else {
//                    used += currentIndex
//                    adjusted += ((currentIndex, value))
//                  }
//                }
//              } catch {
//                case _: NumberFormatException => isValid = false
//              }
//            case _ => isValid = false
//          }
//        }
//
//        if (isValid && adjusted.nonEmpty) {
//          // 环形填充逻辑
//          val sortedRegions = adjusted.sortBy(_._1)
//          val columns = Array.fill(48)(sortedRegions.last._2)
//          val extendedRegions = sortedRegions :+ (sortedRegions.head._1 + 48, sortedRegions.head._2)
//
//          for (i <- sortedRegions.indices) {
//            val (currentIdx, currentVal) = extendedRegions(i)
//            val nextIdx = extendedRegions(i + 1)._1
//            for (j <- currentIdx until nextIdx) {
//              columns(j % 48) = currentVal
//            }
//          }
//
//          // 应用精确序号覆盖
//          sortedRegions.foreach { case (idx, value) =>
//            columns(idx) = value
//          }
//
//          Some(columns.mkString(" "))
//        } else {
//          None
//        }
//      }

//    //划分数据集
//    val sampleData = rdd.takeSample(
//      withReplacement = false,
//      num = 70000,
//      seed = System.currentTimeMillis()
//    )
//    val sampleRDD = sc.parallelize(sampleData, numSlices = 10)

////MoveSim生成结果转换成原始格式
//      val sampleRDD =rdd.map { line =>
//        val fields = line.split("\\s+") // 支持多个空格分隔
//        if (fields.isEmpty) "" else {
//          val buffer = new scala.collection.mutable.ArrayBuffer[String]()
//          var prev = fields.head
//          buffer += s"0;$prev" // 第一个字段必记录
//
//          // 从第二个字段开始检测连续重复
//          for (i <- 1 until fields.length) {
//            if (fields(i) != prev) {
//              buffer += s"$i;${fields(i)}"
//              prev = fields(i)
//            }
//          }
//          buffer.mkString(",")
//        }
//      }
//        .filter(_.nonEmpty)

//    // 1. 读取并构建地理索引广播变量
//    val geoIndex = sc.textFile("D:\\traffic\\TripChain\\experientData\\ExperimentData\\MoveSim\\gps")
//      .zipWithIndex()
//      .map { case (line, idx) =>
//        val Array(lat, lon) = line.split(" ")
//        (idx + 1, (lat, lon)) // 行号从1开始对应索引值
//      }.collectAsMap()
//    val bcGeoIndex = sc.broadcast(geoIndex)
//
//    // 2. 处理原始数据文件
//    val sampleRDD=sc.textFile("D:\\traffic\\TripChain\\result\\geneData\\MoveSim\\adaptive\\part*")
//      .map { line =>
//        line.split(",").map { field =>
//          val parts = field.split(";")
//          val originalId = parts(0)
//          val indexKey = parts(1).toInt
//
//          bcGeoIndex.value.get(indexKey) match {
//            case Some((lat, lon)) => s"$originalId;$lat;$lon"
//            case None => s"$originalId;null;null" // 处理无效索引
//          }
//        }.mkString(",")
//      }





//    Adatrace所需数据格式
val rdd = sc.textFile("D:\\traffic\\TripChain\\experientData\\ExperimentData\\DifferentialPrivacy\\5min\\part-00000")

val sampleRDD = rdd.map { line =>
        // 处理每个字段并转换坐标
        val formattedCoords = line.split(",")
          .flatMap { field =>
            field.split(";") match {
              case Array(_, lon, lat) =>
                try {
                  // 格式化坐标并交换位置
                  val formattedLat = "%.3f".format(lat.toDouble)
                  val formattedLon = "%.3f".format(lon.toDouble)
                  Some(s"$formattedLon,$formattedLat")
                } catch {
                  case _: NumberFormatException => None
                }
              case _ => None
            }
          }
          .mkString(";")

        // 添加前缀并返回结果
        if (formattedCoords.nonEmpty) s">0:$formattedCoords;" else ""
      }
      .filter(_.nonEmpty)

//    插入行索引

    val result=sampleRDD.zipWithIndex() // 生成(原始内容, 行号)
      .flatMap { case (lineContent, lineNumber) =>
        // 生成两行：注释行和原始行
        Seq(
          s"#$lineNumber:",    // 添加注释行
          lineContent          // 保留原始内容
        )
      }

    //AdaTrace生成结果转换成原始形式






    //我们方法需要的数据格式
//    // 处理数据文件生成SpatialRDD
//    val spatialRDD = sc.textFile("D:\\traffic\\TripChain\\experientData\\temp_TripChain\\5min_TripChain\\part-00000")

//    // 读取index文件并构建时间映射
//    val indexLines = sc.textFile("D:\\traffic\\TripChain\\experientData\\time\\adaptive\\index").collect()
//    val indexMap = indexLines.flatMap { line =>
//      val parts = line.split(",")
//      val rangeStr = parts(0)
//      val newTime = parts(1).toInt
//      val (start, end) = if (rangeStr.contains("-")) {
//        val range = rangeStr.split("-").map(_.toInt)
//        (range(0), range(1))
//      } else {
//        val num = rangeStr.toInt
//        (num, num)
//      }
//      (start to end).map(t => (t, newTime))
//    }.toMap
//
//    // 广播时间映射表
//    val bcIndex = sc.broadcast(indexMap)

    // 处理数据文件
//
//      val timeRDD = spatialRDD.map { line =>
//        line.split(",").map { field =>
//          val cols = field.split(";")
//          val time = cols(0).toInt
////          val newTime = bcIndex.value.getOrElse(time,
////            throw new IllegalArgumentException(s"Invalid time: $time"))
//
//          val lon = BigDecimal(cols(2).toDouble)
//            .setScale(3, BigDecimal.RoundingMode.HALF_UP)
//          val lat = BigDecimal(cols(3).toDouble)
//            .setScale(3, BigDecimal.RoundingMode.HALF_UP)
//
//
//          s"$time;$lon;$lat"
//        }.mkString(",")
//      }
//    // 排序并输出结果
//    val sortedRDD = timeRDD.map { line =>
//      val parts = line.split(";")
//      ((parts(1), parts(0).toInt), line)
//    }.sortByKey().map(_._2)
//
    result.coalesce(1).saveAsTextFile(resultPathPrefix)

    sc.stop()
  }

}
