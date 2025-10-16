
package TripChain.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object AvQE {

  def CalAvQE_s(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据 - 统计所有站点
    val synData = sc.textFile(synDataPath)
      .flatMap(line => {
        val fields = line.split(",")
        fields.map(field => {
          val tokens = field.trim.split(" ")
          if (tokens.length >= 2) tokens(1) else "" // 提取站点信息
        })
      })
      .filter(_.nonEmpty) // 过滤空站点
      .map(station => (station, 1)) // (站点, 1)
      .reduceByKey(_ + _) // 统计每个站点的出现次数

    // 读取并处理commuteData数据 - 统计所有站点
    val commuteData = sc.textFile(commuteDataPath)
      .flatMap(line => {
        val fields = line.split(",")
        fields.map(field => {
          val tokens = field.trim.split(" ")
          if (tokens.length >= 2) tokens(1) else "" // 提取站点信息
        })
      })
      .filter(_.nonEmpty) // 过滤空站点
      .map(station => (station, 1)) // (站点, 1)
      .reduceByKey(_ + _) // 统计每个站点的出现次数

    // 全外连接，确保所有站点都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (station, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0) // 通勤数据计数（真实值）
        val synCount = synCountOpt.getOrElse(0) // 合成数据计数
        (station, comCount, synCount)
      }

    // 计算相对误差（分母取真实计数和2000的最大值）
    val errorRDD = joinedCounts.map { case (station, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 800)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (station, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"**************** Average Relative Error for Stations: $avgError ****************")
    println(s"Total stations: $totalCount")
    println("======================================")

    println("======================================")
  }

  def CalAvQE_s1(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 解析新数据格式：提取所有站点信息
    def parseLine(line: String): Array[String] = {
      // 移除起始标记">0:"并按分号分割字段
      line.stripPrefix(">0:").split(";")
        .filter(_.nonEmpty)    // 过滤空字段
        .map(_.trim)           // 去除空格
    }

    // 处理数据并统计站点频率（忽略序号行）
    def processStationCounts(rdd: RDD[String]): RDD[(String, Int)] = {
      rdd.filter(line => line.startsWith(">0:") && line.length > 3) // 过滤有效行
        .flatMap(parseLine)   // 解析为站点数组
        .map(station => (station, 1))
        .reduceByKey(_ + _)   // 统计每个站点的出现次数
    }

    // 处理合成数据
    val synData = processStationCounts(sc.textFile(synDataPath))

    // 处理通勤数据
    val commuteData = processStationCounts(sc.textFile(commuteDataPath))

    // 全外连接，确保所有站点都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (station, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0) // 通勤数据计数（真实值）
        val synCount = synCountOpt.getOrElse(0) // 合成数据计数
        (station, comCount, synCount)
      }

    // 计算相对误差（分母取真实计数和400的最大值）
    val errorRDD = joinedCounts.map { case (station, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 800)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (station, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampler = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val (sumError, count) = sampler.map { case (_, _, _, error) =>
      (error, 1L)
    }.reduce { case ((sum1, count1), (sum2, count2)) =>
      (sum1 + sum2, count1 + count2)
    }

    val avgError = if (count > 0) sumError / count else 0.0

    // 打印结果到控制台
    println("======================================")
    println(s"Average Relative Error for Stations: $avgError")
    println(s"Total stations compared: $totalCount")
    println(s"Sampled stations: $count")
    println("======================================")
  }


  def CalAvQE_st(sc: SparkContext,
                    synDataPath: String,
                    commuteDataPath: String): Unit = {

    // 读取并处理synData数据 - 统计所有字段（时间+站点）
    val synData = sc.textFile(synDataPath)
      .flatMap(line => {
        val fields = line.split(",")
        fields.map(field => field.trim) // 保留完整字段
      })
      .filter(_.nonEmpty) // 过滤空字段
      .map(field => (field, 1)) // (字段, 1)
      .reduceByKey(_ + _) // 统计每个字段的出现次数

    // 读取并处理commuteData数据 - 统计所有字段（时间+站点）
    val commuteData = sc.textFile(commuteDataPath)
      .flatMap(line => {
        val fields = line.split(",")
        fields.map(field => field.trim) // 保留完整字段
      })
      .filter(_.nonEmpty) // 过滤空字段
      .map(field => (field, 1)) // (字段, 1)
      .reduceByKey(_ + _) // 统计每个字段的出现次数

    // 全外连接，确保所有字段都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (field, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0) // 通勤数据计数
        val synCount = synCountOpt.getOrElse(0) // 合成数据计数
        (field, comCount, synCount)
      }

    // 计算相对误差（分母取真实计数和2000的最大值）
    val errorRDD = joinedCounts.map { case (field, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 40)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (field, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"**************** Average Relative Error for Fields: $avgError ****************")
    println("======================================")
    println("======================================")
  }



  def CalAvQE_r(sc: SparkContext,
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

    // 读取并处理commuteData数据 - 统计所有子路径
    val commuteData = sc.textFile(commuteDataPath)
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
      .filter(_.nonEmpty) // 过滤空子路径
      .map(subPath => (subPath, 1)) // (子路径, 1)
      .reduceByKey(_ + _) // 统计每个子路径的出现次数

    // 读取并处理synData数据 - 统计所有子路径
    val synData = sc.textFile(synDataPath)
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
      .filter(_.nonEmpty) // 过滤空子路径
      .map(subPath => (subPath, 1)) // (子路径, 1)
      .reduceByKey(_ + _) // 统计每个子路径的出现次数

    // 全外连接，确保所有子路径都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (subPath, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0) // 通勤数据计数（真实值）
        val synCount = synCountOpt.getOrElse(0) // 合成数据计数
        (subPath, comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case (subPath, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 20)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (subPath, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"**************** Average Relative Error for SubPaths (max length $maxSubPathLength): $avgError ****************")
    println("======================================")
  }

  def CalAvQE_r1(sc: SparkContext,
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

    // 解析新数据格式：提取站点序列
    def parseLine(line: String): Array[String] = {
      // 移除起始标记">0:"并按分号分割字段
      line.stripPrefix(">0:").split(";")
        .filter(_.nonEmpty)    // 过滤空字段
        .map(_.trim)           // 去除空格
    }

    // 处理数据并统计子路径频率
    def processSubPathCounts(rdd: RDD[String]): RDD[(String, Int)] = {
      rdd.filter(line => line.startsWith(">0:") && line.length > 3) // 过滤有效行
        .map(parseLine)   // 解析为站点数组
        .flatMap(generateSubPaths) // 生成所有子路径
        .filter(_.nonEmpty) // 过滤空子路径
        .map(subPath => (subPath, 1)) // (子路径, 1)
        .reduceByKey(_ + _) // 统计每个子路径的出现次数
    }

    // 处理通勤数据
    val commuteData = processSubPathCounts(sc.textFile(commuteDataPath))

    // 处理合成数据
    val synData = processSubPathCounts(sc.textFile(synDataPath))

    // 全外连接，确保所有子路径都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (subPath, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0) // 通勤数据计数（真实值）
        val synCount = synCountOpt.getOrElse(0) // 合成数据计数
        (subPath, comCount, synCount)
      }

    // 计算相对误差（分母取真实计数和20的最大值）
    val errorRDD = joinedCounts.map { case (subPath, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 20)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (subPath, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"**************** Average Relative Error for SubPaths (max length $maxSubPathLength): $avgError ****************")
    println(s"Total subpaths considered: $totalCount")
    println("======================================")
  }

  def CalAvQE_temporal_subpaths(sc: SparkContext,
                                synDataPath: String,
                                commuteDataPath: String,
                                maxSubPathLength: Int = 3): Unit = {

    // 生成所有可能的带时间子路径（长度1到maxSubPathLength）
    def generateTemporalSubPaths(entries: Array[(String, String)]): Seq[String] = {
      val result = scala.collection.mutable.ListBuffer[String]()
      for (length <- 1 to maxSubPathLength) {
        for (start <- 0 to entries.length - length) {
          val subPath = entries.slice(start, start + length)
            .map { case (time, station) => s"$time:$station" }
            .mkString("->")
          result += subPath
        }
      }
      result.toSeq
    }

    // 读取并处理commuteData数据 - 统计所有带时间子路径
    val commuteData = sc.textFile(commuteDataPath)
      .flatMap { line =>
        // 提取(时间, 站点)对
        val entries = line.split(",")
          .map(_.trim.split(" "))
          .filter(_.length >= 2)
          .map(arr => (arr(0), arr(1))) // (时间, 站点)

        // 生成所有带时间子路径
        if (entries.length >= 1) generateTemporalSubPaths(entries) else Seq.empty
      }
      .filter(_.nonEmpty) // 过滤空子路径
      .map(subPath => (subPath, 1)) // (带时间子路径, 1)
      .reduceByKey(_ + _) // 统计每个子路径的出现次数

    // 读取并处理synData数据 - 统计所有带时间子路径
    val synData = sc.textFile(synDataPath)
      .flatMap { line =>
        // 提取(时间, 站点)对
        val entries = line.split(",")
          .map(_.trim.split(" "))
          .filter(_.length >= 2)
          .map(arr => (arr(0), arr(1))) // (时间, 站点)

        // 生成所有带时间子路径
        if (entries.length >= 1) generateTemporalSubPaths(entries) else Seq.empty
      }
      .filter(_.nonEmpty) // 过滤空子路径
      .map(subPath => (subPath, 1)) // (带时间子路径, 1)
      .reduceByKey(_ + _) // 统计每个子路径的出现次数

    // 全外连接，确保所有带时间子路径都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (subPath, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0) // 通勤数据计数（真实值）
        val synCount = synCountOpt.getOrElse(0) // 合成数据计数
        (subPath, comCount, synCount)
      }

    // 计算相对误差（分母取真实计数和20的最大值）
    val errorRDD = joinedCounts.map { case (subPath, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 10) // 使用20作为分母下限
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (subPath, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val (sum, count) = if (!sampledErrors.isEmpty()) {
      sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
    } else {
      (0.0, 0L)
    }

    val avgError = if (count > 0) sum / count else 0.0

    // 打印结果到控制台
    println("=" * 80)
    println(s"带时间子路径平均相对误差结果 (最大子路径长度: $maxSubPathLength)")
    println(s"平均相对误差: $avgError")
    println("=" * 80)

  }

}