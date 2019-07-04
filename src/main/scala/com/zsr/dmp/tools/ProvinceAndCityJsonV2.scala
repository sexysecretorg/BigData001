package com.zsr.dmp.tools

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceAndCityJsonV2 {
  def main(args: Array[String]): Unit = {
    //校验参数的个数
    if (args.length != 1) {
      println(
        """
          |com.zsr.dmp.tools.ProvinceAndCityJsonV2
          |参数：
          |logInputPath
        """.stripMargin)
      sys.exit()
    }
    //1，接收程序的参数
    val logInputPath= args(0)
    //2，创建一个SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val df: DataFrame = sqlContext.read.parquet(logInputPath)
    df.registerTempTable("info")
    val df1: DataFrame = sqlContext.sql("select provincename , cityname , count(1) ct from info group by provincename , cityname")
    //加载配置文件，1，application.conf->application.json->application.properties
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user",load.getString("jdbc.user"))
    props.setProperty("password",load.getString("jdbc.password"))
    df1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),props)
    //6，关闭资源
    sc.stop()
  }

}
