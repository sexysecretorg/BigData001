package com.zsr.dmp.tools

import java.io.File

import com.zsr.dmp.tools.utils.FileUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceAndCityJson {
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
    val sqlContext = new SQLContext(sc);
    val df: DataFrame = sqlContext.read.parquet(logInputPath)
    FileUtil.deleteDir(new File(resultOutputPath));
    df.registerTempTable("info")
    val df1: DataFrame = sqlContext.sql("select provincename , cityname , count(1) ct from info group by provincename , cityname")
    df1.coalesce(1).write.json(resultOutputPath)
    //6，关闭资源
    sc.stop()
  }

}
