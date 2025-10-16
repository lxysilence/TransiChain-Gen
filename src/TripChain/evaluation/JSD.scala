package TripChain.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math._

object JSD {

  def CalJSD_st(sc: SparkContext,
                          synDataPath: String,
                          commuteDataPath: String): Unit = {

    // 提取时间-站点对
    def extractTimeStationPairs(record: String): Array[(String, String)] = {
      record.split(",").map { field =>
        val parts = field.split(" ")
        if (parts.length >= 2) {
          // 提取时间作为第一个元素，站点作为第二个元素
          (parts(0), parts(1))
        } else {
          // 无效字段返回空元组
          ("", "")
        }
      }.filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[(String, String)]): Map[(String, String), Double] = {
      val total = rdd.count().toDouble
      rdd.map(pair => (pair, 1))
        .reduceByKey(_ + _)
        .map { case (pair, count) => (pair, count / total) }
        .collect()
        .toMap
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * math.log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synPairs = sc.textFile(synDataPath)
      .flatMap(extractTimeStationPairs)
      .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

    val synDist = calculateDistribution(synPairs)
    println(s"SynData时间-站点对数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comPairs = sc.textFile(commuteDataPath)
      .flatMap(extractTimeStationPairs)
      .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

    val comDist = calculateDistribution(comPairs)
    println(s"CommuteData时间-站点对数量: ${comDist.size}")

    // 获取所有时间-站点对
    val allPairs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总时间-站点对数量: ${allPairs.size}")

    // 计算JSD散度
    val jsd = allPairs.foldLeft(0.0) { (sum, pair) =>
      val p = synDist.getOrElse(pair, 0.0)
      val q = comDist.getOrElse(pair, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"时间-站点客流分布Jensen-Shannon散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")
  }

  def CalJSD_tripLength(sc: SparkContext,
                        synDataPath: String,
                        commuteDataPath: String): Double = {

    // 计算轨迹长度分布
    def calculateTripLengthDistribution(rdd: RDD[String]): Map[Int, Double] = {
      val lengthCounts = rdd.map { line =>
        (line.split(",").length, 1)
      }.reduceByKey(_ + _)

      val total = lengthCounts.values.sum().toDouble
      lengthCounts.map { case (length, count) =>
        (length, count / total)
      }.collect().toMap
    }

    // 读取并处理synData数据
    val synData = sc.textFile(synDataPath)
    val synDist = calculateTripLengthDistribution(synData)
    println(s"SynData轨迹长度分布统计: ${synDist.keys.min} - ${synDist.keys.max}个站点")
    println(s"SynData轨迹长度类型数: ${synDist.size}")

    // 读取并处理commuteData数据
    val comData = sc.textFile(commuteDataPath)
    val comDist = calculateTripLengthDistribution(comData)
    println(s"CommuteData轨迹长度分布统计: ${comDist.keys.min} - ${comDist.keys.max}个站点")
    println(s"CommuteData轨迹长度类型数: ${comDist.size}")

    // 获取所有可能的轨迹长度并排序
    val allLengths = (synDist.keySet ++ comDist.keySet).toArray.sorted

    // 1. 初始化概率向量
    val p = Array.fill(allLengths.size)(0.0) // 合成数据分布
    val q = Array.fill(allLengths.size)(0.0) // 通勤数据分布

    // 2. 填充概率向量
    for (i <- allLengths.indices) {
      val length = allLengths(i)
      p(i) = synDist.getOrElse(length, 0.0)
      q(i) = comDist.getOrElse(length, 0.0)
    }

    // 3. 计算平均分布
    val m = p.zip(q).map { case (p_i, q_i) => (p_i + q_i) / 2.0 }

    // 4. 计算KL散度
    def klDivergence(p: Array[Double], q: Array[Double]): Double = {
      require(p.length == q.length, "概率向量长度必须相等")

      p.zip(q).foldLeft(0.0) { case (sum, (p_i, q_i)) =>
        if (p_i > 0 && q_i > 0) sum + p_i * math.log(p_i / q_i)
        else if (p_i > 0) Double.PositiveInfinity
        else sum
      }
    }

    // 5. 计算JSD散度
    val klPM = klDivergence(p, m)
    val klQM = klDivergence(q, m)

    var jsd = 0.0
    if (klPM.isInfinite || klQM.isInfinite) {
      // 处理无穷大情况（当分布不相容时）
      jsd = 1.0
      println("警告: 分布不相容导致JSD=1.0（存在只在一种分布中出现的轨迹长度）")
    } else {
      // 标准JSD计算
      jsd = (klPM + klQM) / 2.0

      // 6. 标准化到[0,1]范围
      jsd = jsd / math.log(2)  // 标准化因子

      // 确保结果在[0,1]范围内
      jsd = jsd.max(0.0).min(1.0)
    }

    // 打印结果到控制台
    println("\n======================================")
    println(s"轨迹长度分布Jensen-Shannon散度计算结果")
    println(f"原始JSD值: ${(klPM + klQM)/2}%.6f")
    println(f"标准化JSD值: $jsd%.6f")
    println("======================================")

    // 返回标准化JSD值
    jsd
  }

  def CalJSD_tripLength1(sc: SparkContext,
                        synDataPath: String,
                        commuteDataPath: String): Double = {

    // 计算轨迹长度分布（新格式）
    def calculateTripLengthDistribution(rdd: RDD[String]): Map[Int, Double] = {
      // 过滤有效行并计算长度
      val validLines = rdd.filter(line => line.startsWith(">0:") && line.length > 3)
      val lengthCounts = validLines.map { line =>
        // 移除">0:"前缀，按分号分割并过滤空字符串
        val stations = line.stripPrefix(">0:").split(";").filter(_.nonEmpty)
        (stations.length, 1)
      }.reduceByKey(_ + _)

      val total = lengthCounts.values.sum().toDouble
      lengthCounts.map { case (length, count) =>
        (length, count / total)
      }.collect().toMap
    }

    // 读取并处理synData数据
    val synData = sc.textFile(synDataPath)
    val synDist = calculateTripLengthDistribution(synData)
    println(s"SynData轨迹长度分布统计: 最小长度 ${synDist.keys.min}, 最大长度 ${synDist.keys.max}")
    println(s"SynData轨迹长度类型数: ${synDist.size}")

    // 读取并处理commuteData数据
    val comData = sc.textFile(commuteDataPath)
    val comDist = calculateTripLengthDistribution(comData)
    println(s"CommuteData轨迹长度分布统计: 最小长度 ${comDist.keys.min}, 最大长度 ${comDist.keys.max}")
    println(s"CommuteData轨迹长度类型数: ${comDist.size}")

    // 获取所有可能的轨迹长度并排序
    val allLengths = (synDist.keySet ++ comDist.keySet).toArray.sorted

    // 创建概率向量
    val p = Array.fill(allLengths.size)(0.0) // 合成数据分布
    val q = Array.fill(allLengths.size)(0.0) // 通勤数据分布

    for (i <- allLengths.indices) {
      val length = allLengths(i)
      p(i) = synDist.getOrElse(length, 0.0)
      q(i) = comDist.getOrElse(length, 0.0)
    }

    // 计算平均分布
    val m = p.zip(q).map { case (p_i, q_i) => (p_i + q_i) / 2.0 }

    // 计算KL散度
    def klDivergence(p: Array[Double], q: Array[Double]): Double = {
      var sum = 0.0
      for (i <- p.indices) {
        if (p(i) > 0) {
          if (q(i) > 0) {
            sum += p(i) * math.log(p(i) / q(i))
          } else {
            return Double.PositiveInfinity
          }
        }
      }
      sum
    }

    // 计算JSD
    val klPM = klDivergence(p, m)
    val klQM = klDivergence(q, m)

    val jsd = if (klPM.isInfinite || klQM.isInfinite) {
      println("警告: 分布不相容导致JSD=1.0（存在只在一种分布中出现的轨迹长度）")
      1.0
    } else {
      val rawJSD = (klPM + klQM) / 2.0
      // 标准化：将自然对数转为log2并归一化
      rawJSD / math.log(2)
    }.min(1.0).max(0.0) // 确保在[0,1]范围内

    // 打印结果
    println("\n======================================")
    println(s"轨迹长度分布Jensen-Shannon散度计算结果")
    println(f"标准化JSD值: $jsd%.6f")
    println("======================================")

    jsd
  }


  def CalOD_CorrelationError(sc: SparkContext,
                             synDataPath: String,
                             commuteDataPath: String): Double = {

    // 提取每条轨迹的起点和终点站点
    def extractOriginDestination(record: String): (String, String) = {
      val fields = record.split(",")
      if (fields.nonEmpty) {
        // 提取起点站点
        val firstField = fields.head.split(" ")
        val origin = if (firstField.length >= 2) firstField(1) else "unknown"

        // 提取终点站点
        val lastField = fields.last.split(" ")
        val destination = if (lastField.length >= 2) lastField(1) else "unknown"

        (origin, destination)
      } else {
        ("unknown", "unknown")
      }
    }

    // 计算OD对分布
    def calculateODDistribution(rdd: RDD[(String, String)]): Map[(String, String), Double] = {
      val total = rdd.count().toDouble
      rdd.map(od => (od, 1))
        .reduceByKey(_ + _)
        .map { case (od, count) => (od, count / total) }
        .collect()
        .toMap
    }

    // 计算KL散度（添加平滑处理）
    def klDivergence(p: Double, q: Double): Double = {
      val epsilon = 1e-10
      val pSafe = p + epsilon
      val qSafe = q + epsilon
      pSafe * math.log(pSafe / qSafe)
    }

    // 读取并处理synData数据
    val synOD = sc.textFile(synDataPath)
      .map(extractOriginDestination)
      .filter { case (o, d) => o != "unknown" && d != "unknown" }

    val synDist = calculateODDistribution(synOD)
    println(s"SynData OD对数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comOD = sc.textFile(commuteDataPath)
      .map(extractOriginDestination)
      .filter { case (o, d) => o != "unknown" && d != "unknown" }

    val comDist = calculateODDistribution(comOD)
    println(s"CommuteData OD对数量: ${comDist.size}")

    // 获取所有可能的OD对
    val allODs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总OD对数量: ${allODs.size}")

    // 计算JSD散度
    val jsd = allODs.foldLeft(0.0) { (sum, od) =>
      val p = synDist.getOrElse(od, 0.0)
      val q = comDist.getOrElse(od, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0



    // 打印结果到控制台
    println("======================================")
    println(s"起点-终点相关性误差分析")
    println(f"JSD散度: $jsd%.6f")
    println("======================================")

    // 返回JSD作为主要误差指标
    jsd
  }

  def CalOD_CorrelationError1(sc: SparkContext,
                             synDataPath: String,
                             commuteDataPath: String): Double = {

    // 提取每条轨迹的起点和终点站点（新格式）
    def extractOriginDestination(record: String): (String, String) = {
      if (record.startsWith(">0:") && record.length > 1) {
        val stations = record.stripPrefix(">0:").split(";").filter(_.nonEmpty)
        if (stations.nonEmpty) {
          (stations.head, stations.last) // 第一个和最后一个站点
        } else {
          ("unknown", "unknown")
        }
      } else {
        ("unknown", "unknown")
      }
    }

    // 计算OD对分布
    def calculateODDistribution(rdd: RDD[(String, String)]): Map[(String, String), Double] = {
      val total = rdd.count().toDouble
      rdd.map(od => (od, 1))
        .reduceByKey(_ + _)
        .map { case (od, count) => (od, count / total) }
        .collect()
        .toMap
    }

    // 计算KL散度（添加平滑处理）
    def klDivergence(p: Double, q: Double): Double = {
      val epsilon = 1e-10
      val pSafe = p + epsilon
      val qSafe = q + epsilon
      pSafe * math.log(pSafe / qSafe)
    }

    // 读取并处理synData数据
    val synOD = sc.textFile(synDataPath)
      .filter(line => line.startsWith(">0:") && line.length > 1) // 过滤有效行
      .map(extractOriginDestination)
      .filter { case (o, d) => o != "unknown" && d != "unknown" }

    val synDist = calculateODDistribution(synOD)
    println(s"SynData OD对数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comOD = sc.textFile(commuteDataPath)
      .filter(line => line.startsWith(">0:") && line.length > 1)
      .map(extractOriginDestination)
      .filter { case (o, d) => o != "unknown" && d != "unknown" }

    val comDist = calculateODDistribution(comOD)
    println(s"CommuteData OD对数量: ${comDist.size}")

    // 获取所有可能的OD对
    val allODs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总OD对数量: ${allODs.size}")

    // 计算JSD散度
    val jsd = allODs.foldLeft(0.0) { (sum, od) =>
      val p = synDist.getOrElse(od, 0.0) // 合成数据概率
      val q = comDist.getOrElse(od, 0.0) // 通勤数据概率
      val m = (p + q) / 2.0
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)
      sum + klPM + klQM
    } / 2.0

    // 打印结果
    println("======================================")
    println(s"起点-终点相关性误差分析")
    println(f"JSD散度: $jsd%.6f")
    println("======================================")

    jsd
  }

  def CalJSD_CorrelationError(sc: SparkContext,
                             synDataPath: String,
                             commuteDataPath: String): Double = {

    // 提取每条轨迹的起点和终点的时间和站点信息
    def extractOriginDestination(record: String): ((String, String), (String, String)) = {
      val fields = record.split(",")
      if (fields.nonEmpty) {
        // 提取起点时间和站点
        val firstField = fields.head.split(" ", 2) // 分割成两部分：时间戳和站点
        val startTime = if (firstField.length >= 1) firstField(0) else "unknown"
        val startStation = if (firstField.length >= 2) firstField(1) else "unknown"

        // 提取终点时间和站点
        val lastField = fields.last.split(" ", 2)
        val endTime = if (lastField.length >= 1) lastField(0) else "unknown"
        val endStation = if (lastField.length >= 2) lastField(1) else "unknown"

        ((startTime, startStation), (endTime, endStation))
      } else {
        (("unknown", "unknown"), ("unknown", "unknown"))
      }
    }

    // 计算OD对分布
    def calculateODDistribution(rdd: RDD[((String, String), (String, String))]): Map[((String, String), (String, String)), Double] = {
      val total = rdd.count().toDouble
      rdd.map(od => (od, 1))
        .reduceByKey(_ + _)
        .map { case (od, count) => (od, count / total) }
        .collect()
        .toMap
    }

    // 计算KL散度（添加平滑处理）
    def klDivergence(p: Double, q: Double): Double = {
      val epsilon = 1e-10
      val pSafe = p + epsilon
      val qSafe = q + epsilon
      pSafe * math.log(pSafe / qSafe)
    }

    // 读取并处理synData数据
    val synOD = sc.textFile(synDataPath)
      .map(extractOriginDestination)
      .filter { case ((st, ss), (et, es)) =>
        st != "unknown" && ss != "unknown" && et != "unknown" && es != "unknown"
      }

    val synDist = calculateODDistribution(synOD)
    println(s"SynData OD对数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comOD = sc.textFile(commuteDataPath)
      .map(extractOriginDestination)
      .filter { case ((st, ss), (et, es)) =>
        st != "unknown" && ss != "unknown" && et != "unknown" && es != "unknown"
      }

    val comDist = calculateODDistribution(comOD)
    println(s"CommuteData OD对数量: ${comDist.size}")

    // 获取所有可能的OD对
    val allODs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总OD对数量: ${allODs.size}")

    // 计算JSD散度
    val jsd = allODs.foldLeft(0.0) { (sum, od) =>
      val p = synDist.getOrElse(od, 0.0)
      val q = comDist.getOrElse(od, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"带时间的起点-终点相关性误差分析")
    println(f"JSD散度: $jsd%.6f")
    println("======================================")

    // 返回JSD作为主要误差指标
    jsd
  }


}