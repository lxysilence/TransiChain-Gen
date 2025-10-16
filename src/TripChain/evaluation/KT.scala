package TripChain.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object KT {
  def AdaKT_st(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 处理合成数据 - 提取所有时间和站点
    val synCounts = sc.textFile(synDataPath)
      .flatMap { line =>
        line.split(",").flatMap { field =>
          val parts = field.split(" ")
          if (parts.length >= 2) {
            Seq(parts(0), parts(1)) // 提取时间和站点
          } else {
            Seq.empty[String]
          }
        }
      }
      .map(item => (item, 1))
      .reduceByKey(_ + _)

    // 处理真实通勤数据 - 同样提取所有时间和站点
    val comCounts = sc.textFile(commuteDataPath)
      .flatMap { line =>
        line.split(",").flatMap { field =>
          val parts = field.split(" ")
          if (parts.length >= 2) {
            Seq(parts(0), parts(1)) // 提取时间和站点
          } else {
            Seq.empty[String]
          }
        }
      }
      .map(item => (item, 1))
      .reduceByKey(_ + _)

    // 获取各自的前150热门项
    val top150Syn = synCounts
      .sortBy(-_._2) // 按计数降序
      .map(_._1)      // 只保留项目名称
      .take(150)      // 取前150
      .toSet

    val top150Com = comCounts
      .sortBy(-_._2)
      .map(_._1)
      .take(150)
      .toSet

    // 获取共同的热门项（前150的交集）
    val commonItems = top150Syn.intersect(top150Com).toSeq.sorted

    // 获取共同项目的计数映射
    val comCountMap = comCounts.collectAsMap()
    val synCountMap = synCounts.collectAsMap()

    // 构建排名序列
    val comRankings = comCounts
      .filter(item => commonItems.contains(item._1)) // 只考虑共同项目
      .sortBy(-_._2) // 按计数降序
      .zipWithIndex() // 添加排名
      .map { case ((item, _), idx) => (item, idx) }
      .collectAsMap()

    val synRankings = synCounts
      .filter(item => commonItems.contains(item._1)) // 只考虑共同项目
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((item, _), idx) => (item, idx) }
      .collectAsMap()

    // 计算KT系数
    var concordant = 0
    var discordant = 0
    val n = commonItems.size

    for (i <- 0 until n; j <- i + 1 until n) {
      val itemA = commonItems(i)
      val itemB = commonItems(j)

      // 获取两个项目在两个数据集中的排名
      val comRankA = comRankings(itemA)
      val comRankB = comRankings(itemB)
      val synRankA = synRankings(itemA)
      val synRankB = synRankings(itemB)

      // 比较排名顺序
      val comOrder = comRankA.compareTo(comRankB)
      val synOrder = synRankA.compareTo(synRankB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }
      // 相等情况忽略
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    println("======================================")
    println(s"***** Kendall Tau for Top 150 Times & Stations: $kt *****")
    println(s"***** Common Items Count: $n *****")
    println("======================================")
  }

  def AdaKT_s1(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 处理合成数据 - 提取所有站点（每个字段的第二个单词）
    val synCounts = sc.textFile(synDataPath)
      .flatMap { line =>
        line.split(",").flatMap { field =>
//          val parts = field.split("\\s+")
          val parts = field.split(";")
          if (parts.length >= 2) {
            Seq(parts(1)) // 只提取站点信息（第二个单词）
          } else {
            Seq.empty[String]
          }
        }
      }
      .map(station => (station, 1))
      .reduceByKey(_ + _)

    // 处理真实通勤数据 - 同样只提取站点信息
    val comCounts = sc.textFile(commuteDataPath)
      .flatMap { line =>
        line.split(",").flatMap { field =>
          val parts = field.split(";")
          if (parts.length >= 2) {
            Seq(parts(1)) // 只提取站点信息（第二个单词）
          } else {
            Seq.empty[String]
          }
        }
      }
      .map(station => (station, 1))
      .reduceByKey(_ + _)

    // 获取各自的前150热门站点
    val top150Syn = synCounts
      .sortBy(-_._2) // 按计数降序
      .map(_._1)      // 只保留站点名称
      .take(150)      // 取前150
      .toSet

    val top150Com = comCounts
      .sortBy(-_._2)
      .map(_._1)
      .take(150)
      .toSet

    // 获取共同的热门站点（前150的交集）
    val commonStations = top150Syn.intersect(top150Com).toSeq.sorted

    // 获取共同站点的计数映射
    val comCountMap = comCounts.collectAsMap()
    val synCountMap = synCounts.collectAsMap()

    // 构建排名序列
    val comRankings = comCounts
      .filter(station => commonStations.contains(station._1)) // 只考虑共同站点
      .sortBy(-_._2) // 按计数降序
      .zipWithIndex() // 添加排名
      .map { case ((station, _), idx) => (station, idx) }
      .collectAsMap()

    val synRankings = synCounts
      .filter(station => commonStations.contains(station._1)) // 只考虑共同站点
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((station, _), idx) => (station, idx) }
      .collectAsMap()

    // 计算KT系数
    var concordant = 0
    var discordant = 0
    val n = commonStations.size

    for (i <- 0 until n; j <- i + 1 until n) {
      val stationA = commonStations(i)
      val stationB = commonStations(j)

      // 获取两个站点在两个数据集中的排名
      val comRankA = comRankings(stationA)
      val comRankB = comRankings(stationB)
      val synRankA = synRankings(stationA)
      val synRankB = synRankings(stationB)

      // 比较排名顺序
      val comOrder = comRankA.compareTo(comRankB)
      val synOrder = synRankA.compareTo(synRankB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }
      // 相等情况忽略
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    println("======================================")
    println(s"***** Kendall Tau for Top 150 Stations: $kt *****")
    println("======================================")
  }

//  def AdaKT_s(sc: SparkContext,
//              synDataPath: String,
//              commuteDataPath: String): Unit = {
//
//    // 处理新数据格式：过滤行首标记，提取经纬度作为站点标识
//    def processLine(line: String): Array[String] = {
//      if (line.startsWith(">0:")) {
//        line.substring(3)  // 移除">0:"前缀
//          .split(";")      // 按分号分割站点
//          .filter(_.nonEmpty)  // 过滤空字段
//          .map(_.trim)     // 清理空格
//      } else {
//        Array.empty[String]
//      }
//    }
//
//    // 处理合成数据 - 提取所有站点（经纬度字符串）
//    val synCounts = sc.textFile(synDataPath)
//      .flatMap(processLine)
//      .map(station => (station, 1))
//      .reduceByKey(_ + _)
//
//    // 处理真实通勤数据 - 同样提取经纬度站点
//    val comCounts = sc.textFile(commuteDataPath)
//      .flatMap(processLine)
//      .map(station => (station, 1))
//      .reduceByKey(_ + _)
//
//    // 获取各自的前150热门站点
//    val top150Syn = synCounts
//      .sortBy(-_._2) // 按计数降序
//      .map(_._1)      // 只保留站点名称
//      .take(150)      // 取前150
//      .toSet
//
//    val top150Com = comCounts
//      .sortBy(-_._2)
//      .map(_._1)
//      .take(150)
//      .toSet
//
//    // 获取共同的热门站点（前150的交集）
//    val commonStations = top150Syn.intersect(top150Com).toSeq.sorted
//
//    // 获取共同站点的计数映射
//    val comCountMap = comCounts.collectAsMap()
//    val synCountMap = synCounts.collectAsMap()
//
//    // 构建排名序列
//    val comRankings = comCounts
//      .filter(station => commonStations.contains(station._1)) // 只考虑共同站点
//      .sortBy(-_._2) // 按计数降序
//      .zipWithIndex() // 添加排名
//      .map { case ((station, _), idx) => (station, idx) }
//      .collectAsMap()
//
//    val synRankings = synCounts
//      .filter(station => commonStations.contains(station._1)) // 只考虑共同站点
//      .sortBy(-_._2)
//      .zipWithIndex()
//      .map { case ((station, _), idx) => (station, idx) }
//      .collectAsMap()
//
//    // 计算KT系数
//    var concordant = 0
//    var discordant = 0
//    val n = commonStations.size
//
//    for (i <- 0 until n; j <- i + 1 until n) {
//      val stationA = commonStations(i)
//      val stationB = commonStations(j)
//
//      // 获取两个站点在两个数据集中的排名
//      val comRankA = comRankings.getOrElse(stationA, Int.MaxValue)
//      val comRankB = comRankings.getOrElse(stationB, Int.MaxValue)
//      val synRankA = synRankings.getOrElse(stationA, Int.MaxValue)
//      val synRankB = synRankings.getOrElse(stationB, Int.MaxValue)
//
//      // 比较排名顺序
//      val comOrder = comRankA.compareTo(comRankB)
//      val synOrder = synRankA.compareTo(synRankB)
//
//      if (comOrder * synOrder > 0) {
//        concordant += 1
//      } else if (comOrder * synOrder < 0) {
//        discordant += 1
//      }
//      // 相等情况忽略
//    }
//
//    // 计算KT系数
//    val kt = if (n < 2) 0.0 else {
//      (concordant - discordant).toDouble / (n * (n - 1) / 2)
//    }
//
//    println("======================================")
//    println(s"***** Kendall Tau for Top 150 Stations: $kt *****")
//    println(s"共同站点数量: $n")
//    println(s"一致对数: $concordant, 不一致对数: $discordant")
//    println("======================================")
//  }

  def AdaKT_subpaths(sc: SparkContext,
                     synDataPath: String,
                     commuteDataPath: String): Unit = {

    // 提取所有可能的站点子序列（长度≥2）
    def extractSubPaths(record: String): Seq[String] = {
      val fields = record.split(",")
      // 提取每个字段的站点（忽略时间）
      val stations = fields.flatMap { field =>
        val parts = field.split("\\s+")
        if (parts.length >= 2) Some(parts(1)) else None
      }

      // 生成所有连续子序列（长度≥2）
      val subPaths = for {
        start <- 0 until stations.length
        end <- (start + 1) until stations.length
      } yield stations.slice(start, end + 1).mkString("-")

      subPaths
    }

    // 处理合成数据 - 提取子路径
    val synCounts = sc.textFile(synDataPath)
      .flatMap(extractSubPaths)
      .map(subPath => (subPath, 1))
      .reduceByKey(_ + _)

    // 处理真实通勤数据 - 同样提取子路径
    val comCounts = sc.textFile(commuteDataPath)
      .flatMap(extractSubPaths)
      .map(subPath => (subPath, 1))
      .reduceByKey(_ + _)

    // 获取各自的前150热门子路径
    val top150Syn = synCounts
      .sortBy(-_._2) // 按计数降序
      .map(_._1)      // 只保留子路径
      .take(10)      // 取前150
      .toSet

    val top150Com = comCounts
      .sortBy(-_._2)
      .map(_._1)
      .take(10)
      .toSet

    // 获取共同的热门子路径（前150的交集）
    val commonSubPaths = top150Syn.intersect(top150Com).toSeq.sorted

    // 获取共同子路径的计数映射
    val comCountMap = comCounts.collectAsMap()
    val synCountMap = synCounts.collectAsMap()

    // 构建排名序列
    val comRankings = comCounts
      .filter(subPath => commonSubPaths.contains(subPath._1)) // 只考虑共同子路径
      .sortBy(-_._2) // 按计数降序
      .zipWithIndex() // 添加排名
      .map { case ((subPath, _), idx) => (subPath, idx) }
      .collectAsMap()

    val synRankings = synCounts
      .filter(subPath => commonSubPaths.contains(subPath._1)) // 只考虑共同子路径
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((subPath, _), idx) => (subPath, idx) }
      .collectAsMap()

    // 计算KT系数
    var concordant = 0
    var discordant = 0
    val n = commonSubPaths.size

    for (i <- 0 until n; j <- i + 1 until n) {
      val subPathA = commonSubPaths(i)
      val subPathB = commonSubPaths(j)

      // 获取两个子路径在两个数据集中的排名
      val comRankA = comRankings.getOrElse(subPathA, Long.MaxValue)
      val comRankB = comRankings.getOrElse(subPathB, Long.MaxValue)
      val synRankA = synRankings.getOrElse(subPathA, Long.MaxValue)
      val synRankB = synRankings.getOrElse(subPathB, Long.MaxValue)

      // 比较排名顺序
      val comOrder = comRankA.compareTo(comRankB)
      val synOrder = synRankA.compareTo(synRankB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }
      // 相等情况忽略
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    // 打印结果
    println("======================================")
    println(s"***** Kendall Tau for Top 150 Subpaths: $kt *****")
    println(s"***** Common Subpaths Count: $n *****")
    println("======================================")

  }


  def CalKT_r(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String,
              maxSubPathLength: Int = 3): Unit = {

    // 生成所有可能的子路径（长度1到maxSubPathLength）
    def generateSubPaths(stations: Array[String]): Seq[String] = {
      val result = scala.collection.mutable.ListBuffer[String]()
      for (length <- 1 to maxSubPathLength) {
        for (start <- 0 to stations.length - length) {
          val subPath = stations.slice(start, start + length).mkString("->")
          result += subPath
        }
      }
      result.toSeq
    }

    // 读取并处理synData数据（统计子路径）
    val synCounts = sc.textFile(synDataPath)
      .flatMap { line =>
        // 提取站点序列并忽略时间信息
        val stations = line.split(",")
          .flatMap(_.split(" "))
          .grouped(2)
          .map(arr => arr(1))
          .toArray

        // 生成所有子路径
        generateSubPaths(stations)
      }
      .map(subPath => (subPath, 1)) // (子路径, 1)
      .reduceByKey(_ + _) // 统计每个子路径的出现次数
      .sortBy(-_._2) // 按计数降序排序

    // 读取并处理commuteData数据（统计子路径）
    val comCounts = sc.textFile(commuteDataPath)
      .flatMap { line =>
        // 提取站点序列并忽略时间信息
        val stations = line.split(",")
          .flatMap(_.split(" "))
          .grouped(2)
          .map(arr => arr(1))
          .toArray

        // 生成所有子路径
        generateSubPaths(stations)
      }
      .map(subPath => (subPath, 1)) // (子路径, 1)
      .reduceByKey(_ + _) // 统计每个子路径的出现次数
      .sortBy(-_._2) // 按计数降序排序

    // 取commuteData中前150的热门子路径
    val top150SubPaths = comCounts.take(150).map(_._1).toSet
    val broadcastTop150 = sc.broadcast(top150SubPaths)

    // 过滤出两个数据集中都存在的子路径
    val filteredSyn = synCounts.filter { case (subPath, _) =>
      broadcastTop150.value.contains(subPath)
    }.collect().toMap

    val filteredCom = comCounts.filter { case (subPath, _) =>
      broadcastTop150.value.contains(subPath)
    }.collect().toMap

    // 获取共同存在的子路径列表
    val commonSubPaths = filteredSyn.keySet.intersect(filteredCom.keySet).toArray

    // 计算KT系数
    val n = commonSubPaths.length
    var concordant = 0
    var discordant = 0

    for (i <- 0 until n; j <- i + 1 until n) {
      val a = commonSubPaths(i)
      val b = commonSubPaths(j)

      val comA = filteredCom(a)
      val comB = filteredCom(b)
      val synA = filteredSyn(a)
      val synB = filteredSyn(b)

      val comOrder = comA.compareTo(comB)
      val synOrder = synA.compareTo(synB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      } else if (comOrder == 0 && synOrder == 0) {
        concordant += 1
      } else {
        discordant += 1
      }
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0
    else (concordant - discordant).toDouble / (n * (n - 1) / 2)

    // 打印结果到控制台
    println("======================================")
    println(s"SubPath KT Coefficient: $kt")
    println(s"Concordant pairs: $concordant")
    println(s"Discordant pairs: $discordant")
    println(s"Total comparable pairs: ${n * (n - 1) / 2}")
    println(s"Common subpaths considered: $n")
    println(s"Max subpath length: $maxSubPathLength")
    println("======================================")
  }


  def generateSubPaths(stations: Array[String], maxSubPathLength: Int): Seq[String] = {
    (1 to maxSubPathLength).flatMap { length =>
      (0 to stations.length - length).map { start =>
        stations.slice(start, start + length).mkString("->")
      }
    }
  }

  def CalKT_r1(sc: SparkContext,
               synDataPath: String,
               commuteDataPath: String,
               maxSubPathLength: Int = 3): Unit = {

    // 处理合成数据 - 提取子路径
    val synCounts = sc.textFile(synDataPath)
      .filter(!_.startsWith("#"))  // 跳过序号行
      .filter(_.startsWith(">0:")) // 只处理数据行
      .flatMap { line =>
        // 移除">0:"前缀并按分号分割字段
        val fields = line.stripPrefix(">0:").split(";").filter(_.nonEmpty)
        // 生成所有可能的子路径
        val subPaths = generateSubPaths(fields, maxSubPathLength)
        subPaths
      }
      .map(subPath => (subPath, 1))
      .reduceByKey(_ + _)

    // 处理真实通勤数据 - 同样提取子路径
    val comCounts = sc.textFile(commuteDataPath)
      .filter(!_.startsWith("#"))  // 跳过序号行
      .filter(_.startsWith(">0:")) // 只处理数据行
      .flatMap { line =>
        val fields = line.stripPrefix(">0:").split(";").filter(_.nonEmpty)
        val subPaths = generateSubPaths(fields, maxSubPathLength)
        subPaths
      }
      .map(subPath => (subPath, 1))
      .reduceByKey(_ + _)

    // 获取各自的前150热门子路径
    val top150Syn = synCounts
      .sortBy(-_._2) // 按计数降序
      .map(_._1)      // 只保留子路径
      .take(150)      // 取前150
      .toSet

    val top150Com = comCounts
      .sortBy(-_._2)
      .map(_._1)
      .take(150)
      .toSet

    // 获取共同的热门子路径（前150的交集）
    val commonSubPaths = top150Syn.intersect(top150Com).toSeq.sorted

    // 获取共同子路径的计数映射
    val comCountMap = comCounts.collectAsMap()
    val synCountMap = synCounts.collectAsMap()

    // 构建排名序列
    val comRankings = comCounts
      .filter(subPath => commonSubPaths.contains(subPath._1)) // 只考虑共同子路径
      .sortBy(-_._2) // 按计数降序
      .zipWithIndex() // 添加排名
      .map { case ((subPath, _), idx) => (subPath, idx) }
      .collectAsMap()

    val synRankings = synCounts
      .filter(subPath => commonSubPaths.contains(subPath._1)) // 只考虑共同子路径
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((subPath, _), idx) => (subPath, idx) }
      .collectAsMap()

    // 计算KT系数
    var concordant = 0
    var discordant = 0
    val n = commonSubPaths.size

    for (i <- 0 until n; j <- i + 1 until n) {
      val subPathA = commonSubPaths(i)
      val subPathB = commonSubPaths(j)

      // 获取两个子路径在两个数据集中的排名
      val comRankA = comRankings(subPathA)
      val comRankB = comRankings(subPathB)
      val synRankA = synRankings(subPathA)
      val synRankB = synRankings(subPathB)

      // 比较排名顺序
      val comOrder = comRankA.compareTo(comRankB)
      val synOrder = synRankA.compareTo(synRankB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }
      // 相等情况忽略
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    println("======================================")
    println(s"***** Kendall Tau for Top 150 SubPaths: $kt *****")
    println("======================================")
  }


  def CalKT_h(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 解析新数据格式：提取所有站点ID
    def parseLine(line: String): Array[String] = {
      line.split(",")
        .flatMap(_.split(" ")) // 分割字段和单词
        .filter(_.nonEmpty)    // 过滤空字段
        .grouped(2)            // 每两个元素一组（时间戳+站点）
        .map(arr => arr(1))    // 提取站点ID
        .toArray
    }

    // 统计站点出现频率（去重计数：每个轨迹中站点只计1次）
    def countStations(rdd: RDD[String]): RDD[(String, Int)] = {
      rdd.map(parseLine)
        .flatMap(stations => stations.distinct) // 每行去重后站点
        .map(station => (station, 1))
        .reduceByKey(_ + _)
        .sortBy(-_._2) // 按频率降序排序
    }

    // 处理合成数据
    val synCounts = countStations(sc.textFile(synDataPath))

    // 处理通勤数据
    val comCounts = countStations(sc.textFile(commuteDataPath))

    // 取通勤数据中前150的站点
    val top150Stations = comCounts.take(150).map(_._1).toSet
    val broadcastTop150 = sc.broadcast(top150Stations)

    // 过滤共同站点
    val filteredSyn = synCounts.filter { case (station, _) =>
      broadcastTop150.value.contains(station)
    }.collect().toMap

    val filteredCom = comCounts.filter { case (station, _) =>
      broadcastTop150.value.contains(station)
    }.collect().toMap

    // 获取共同站点列表
    val commonStations = filteredSyn.keySet.intersect(filteredCom.keySet).toArray

    // 计算KT系数
    val n = commonStations.length
    var concordant = 0
    var discordant = 0

    for (i <- 0 until n; j <- i+1 until n) {
      val a = commonStations(i)
      val b = commonStations(j)

      val comA = filteredCom.getOrElse(a, 0)
      val comB = filteredCom.getOrElse(b, 0)
      val synA = filteredSyn.getOrElse(a, 0)
      val synB = filteredSyn.getOrElse(b, 0)

      val comCompare = comA.compareTo(comB)
      val synCompare = synA.compareTo(synB)

      if (comCompare * synCompare > 0) {
        concordant += 1
      } else if (comCompare * synCompare < 0) {
        discordant += 1
      } else if (comCompare == 0 && synCompare == 0) {
        concordant += 1
      } else {
        discordant += 1
      }
    }

    // 计算KT系数（确保分母不为零）
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    // 打印结果
    println("======================================")
    println(s"Station KT Coefficient: $kt")
    println(s"Concordant pairs: $concordant")
    println(s"Discordant pairs: $discordant")
    println(s"Total comparable pairs: ${n*(n-1)/2}")
    println(s"Common stations count: $n")
    println("======================================")
  }

  def CalKT_h1(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 解析新数据格式：提取所有站点ID
    def parseLine(line: String): Array[String] = {
      // 移除起始标记">0:"并按分号分割字段
      val fields = line.stripPrefix(">0:").split(";")

      fields
        .filter(_.nonEmpty)    // 过滤空字段
        .map(_.trim)           // 去除空格
        .distinct              // 单条轨迹内去重
    }

    // 统计站点出现频率
    def countStations(rdd: RDD[String]): RDD[(String, Int)] = {
      // 过滤掉序号行（以#开头）和空行
      val filteredRDD = rdd.filter(line =>
        line.startsWith(">0:") && line.length > 3
      )

      filteredRDD.map(parseLine)
        .flatMap(stations => stations)  // 展平所有站点
        .map(station => (station, 1))
        .reduceByKey(_ + _)             // 聚合计数
        .sortBy(-_._2)                  // 按频率降序排序
    }

    // 处理合成数据
    val synCounts = countStations(sc.textFile(synDataPath))

    // 处理通勤数据
    val comCounts = countStations(sc.textFile(commuteDataPath))

    // 取通勤数据中前150的站点
    val top150Stations = comCounts.take(150).map(_._1).toSet
    val broadcastTop150 = sc.broadcast(top150Stations)

    // 过滤共同站点
    val filteredSyn = synCounts.filter { case (station, _) =>
      broadcastTop150.value.contains(station)
    }.collect().toMap

    val filteredCom = comCounts.filter { case (station, _) =>
      broadcastTop150.value.contains(station)
    }.collect().toMap

    // 获取共同站点列表
    val commonStations = filteredSyn.keySet.intersect(filteredCom.keySet).toArray

    // 计算KT系数
    val n = commonStations.length
    var concordant = 0
    var discordant = 0

    for (i <- 0 until n; j <- i+1 until n) {
      val a = commonStations(i)
      val b = commonStations(j)

      val comA = filteredCom.getOrElse(a, 0)
      val comB = filteredCom.getOrElse(b, 0)
      val synA = filteredSyn.getOrElse(a, 0)
      val synB = filteredSyn.getOrElse(b, 0)

      val comCompare = comA.compareTo(comB)
      val synCompare = synA.compareTo(synB)

      if (comCompare * synCompare > 0) {
        concordant += 1
      } else if (comCompare * synCompare < 0) {
        discordant += 1
      }
      // 频次相等时不计数（标准KT处理方式）
    }

    // 计算KT系数
    val totalPairs = n * (n - 1) / 2
    val kt = if (totalPairs > 0) {
      (concordant - discordant).toDouble / totalPairs
    } else 0.0

    // 打印结果
    println("======================================")
    println(s"Station KT Coefficient: $kt")
    println(s"Concordant pairs: $concordant")
    println(s"Discordant pairs: $discordant")
    println(s"Total comparable pairs: $totalPairs")
    println(s"Common stations count: $n")
    println("======================================")
  }


}