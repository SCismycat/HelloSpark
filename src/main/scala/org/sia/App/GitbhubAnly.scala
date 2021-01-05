package org.sia.App

import org.apache.spark.sql.SparkSession

object GitbhubAnly {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("github push analyse")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val homeDir = "E:\\IdeaProject\\hellospark\\src\\main\\resources\\data\\"
    val inputPath = homeDir + "2015-03-01-0.json"
    val ghLog = spark.read.json(inputPath)
    val pushes = ghLog.filter("type='PushEvent'")
    pushes.printSchema()
    println("all event:" + ghLog.count())
    println("only pushes" + pushes.count())
    pushes.show(5)

    //数据汇总
    print("数据汇总..")
    val grouped = pushes.groupBy("actor.login").count
    grouped.show(5)

    //输出并排序
    val ordered = grouped.orderBy(grouped("count").desc) //等同于调用了apply
    ordered.show(5)


  }
}
