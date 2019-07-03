package com.zsr.dmp.tools

import com.zsr.dmp.tools.utils.{RowUtil, SchemaUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * zhouyi
  * 2019-06-09 21:15:35
  *          将原始的日志文件格式转换为parquet格式文件
  *          使用snappy压缩格式
  **/
object Bzip2Parquet {
  def main(args: Array[String]): Unit = {
    //校验参数的个数
    if (args.length != 3) {
      println(
        """
          |com.zsr.dmp.tools.Bzip2Parquet
          |参数：
          |logInputPath
          |parquet compression <uncompressed，snappy，gzip，lzo>
          |resultOutputPath
        """.stripMargin)
      sys.exit()
    }
    //1，接收程序的参数
    val Array(logInputPath, compressionType, resultOutputPath) = args
    //2，创建一个SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    //    conf.registerKryoClasses()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    //3，读取日志数据
    val rawData = sc.textFile(logInputPath)
    val schame = SchemaUtil.getStructType;
    //4，根据业务需求对数据进行过滤抽取转换 ETL
    val rowRdd: RDD[Row] = rawData
      .map(line => line.split(",", line.length))
      .filter(_.length >= 85)
      .map(arr => {
        RowUtil.getRow(arr)
      })
    val df: DataFrame = sqlContext.createDataFrame(rowRdd, schame)
    //5，将结果存储到本地磁盘中
    df.write.parquet(resultOutputPath)
    //6，关闭资源
    sc.stop()
  }

}
