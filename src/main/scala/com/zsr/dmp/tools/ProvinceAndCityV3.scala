package com.zsr.dmp.tools

import java.io.File

import com.zsr.dmp.tools.utils.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceAndCityV3 {
  def main(args: Array[String]): Unit = {
    //校验参数的个数
    if (args.length != 2) {
      println(
        """
          |com.zsr.dmp.tools.ProvinceAndCityJson
          |参数：
          |logInputPath
          |resultOutputPath
        """.stripMargin)
      sys.exit()
    }
    //1，接收程序的参数
    val Array(logInputPath, resultOutputPath) = args
    //2，创建一个SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    FileUtil.deleteDir(new File(resultOutputPath))
    sc.textFile("data").map(line => line.split(",", -1)).filter(_.length >= 85)
      .map(arr => ((arr(24), arr(25)), 1))
      .reduceByKey(_ + _)
      .map(item => {
        item._1._1 + "," + item._1._2 + "," + item._2
      })
      .saveAsTextFile(resultOutputPath)
    sc.stop()
  }

}
