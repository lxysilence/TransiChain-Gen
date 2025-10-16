package TripChain.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object privacy {

  def infer(sc: SparkContext, synDataPath: String): Unit = {
    // 读取原始数据
    val data: RDD[String] = sc.textFile(synDataPath)

    // 1. 统计所有字段个数（记录总数）
    val fieldCounts = data.map(_.split(",").length)
    val totalFields = fieldCounts.sum().toLong

    // 2. 按行去重（相同轨迹只保留一行）
    val distinctData = data.distinct()

    // 3. 创建所有字段的RDD（每个字段单独一条记录）
    val allFields = distinctData.flatMap(line => line.split(","))

    // 统计每个字段的出现频率
    val fieldFrequencies = allFields.map(field => (field, 1))
      .reduceByKey(_ + _)

    // 计算唯一识别计数（出现频率为1的字段）
    val uniqueFields = fieldFrequencies.filter { case (_, count) => count == 1 }
    val uniqueCount = uniqueFields.count()

    // 4. 计算唯一识别率
    val uniqueRate = if (totalFields > 0) uniqueCount.toDouble / totalFields else 0.0

    // 打印结果
    println("======================================")
    println("路径唯一识别率计算结果")
    println(s"总字段个数: $totalFields")
    println(s"唯一识别字段个数: $uniqueCount")
    println(f"唯一识别率: $uniqueRate%.6f")
    println("======================================")
  }




  def Ada_infer(sc: SparkContext, synDataPath: String): Unit
  = {
    // 读取原始数据
    val rawData: RDD[String
    ] = sc.textFile(synDataPath)

    // 1. 预处理数据：过滤掉编号行(#开头的行)，并移除每行的>0:前缀
    val
    processedData = rawData
      .filter(line => !line.startsWith("#") && line.startsWith(">0:")) // 过滤编号行和数据行
      .map(line => line.substring(3)) // 移除">0:"前缀

    // 2. 统计所有字段个数（记录总数）
    val fieldCounts = processedData.map(_.split(";"
    ).count(field => field.nonEmpty))
    val
    totalFields = fieldCounts.sum().toLong

    // 3. 按行去重（相同轨迹只保留一行）
    val
    distinctData = processedData.distinct()

    // 4. 创建所有字段的RDD（每个字段单独一条记录）
    val
    allFields = distinctData.flatMap { line =>
      line.split(";"
        )
        .filter(_.nonEmpty) // 过滤空字段
        .map(_.trim)        // 去除前后空格
    }

    // 统计每个字段的出现频率
    val fieldFrequencies = allFields.map(field => (field, 1
      ))
      .reduceByKey(_ + _)

    // 计算唯一识别计数（出现频率为1的字段）
    val uniqueFields = fieldFrequencies.filter { case (_, count) => count == 1
    }
    val
    uniqueCount = uniqueFields.count()

    // 5. 计算唯一识别率
    val uniqueRate = if (totalFields > 0) uniqueCount.toDouble / totalFields else 0.0

    // 打印结果
    println("======================================"
    )
    println("路径唯一识别率计算结果"
    )
    println(s"总字段个数: $totalFields"
    )
    println(s"唯一识别字段个数: $uniqueCount"
    )
    println(f"唯一识别率: $uniqueRate%.6f"
    )
    println("======================================"
    )
  }
}