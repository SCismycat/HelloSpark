package org.sia.App

import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    val sc = spark.sparkContext
    val ghLog = spark.read.json(args(0))
    val pushes = ghLog.filter("type='PushEvent'") //按照type是pushevent的类型筛选；
    val grouped = pushes.groupBy("actor.login").count // 结果按照actor.login条件进行聚合
    val ordered = grouped.orderBy(grouped("count").desc) // 首先，按照count聚合，并降序排序，再按照某一列进行排序

    val empPath = args(1)
    val employees = Set()++(
      for {
        line <-fromFile(empPath).getLines
      } yield line.trim)

    val bcEmployees = sc.broadcast(employees) //广播employee集合
    import spark.implicits._
    val isEmp =user=>bcEmployees.value.contains(user)
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
    filtered.write.format(args(3)).save(args(2))

  }
}
