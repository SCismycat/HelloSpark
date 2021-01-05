package org.sia.App

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

object testDemo {

  def main(args:Array[String]){
    val spark = SparkSession
      .builder
      .appName("FPGrowthDemo").master("local")
      .config("spark.sql.warehouse.dir", "C:\\study\\sparktest")
      .getOrCreate()

    import spark.implicits._
    val dataset = spark.createDataset(Seq(
      "D E",
      "D E",
      "A B C",
      "A B C E",
      "B E",
      "C D E",
      "A B C",
      "A B C E",
      "B E",
      "F G",
      "D F")
    ).map(t => t.split(" ")).toDF("items")
    println(dataset.show(5))

    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.3)
    //val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.3) .setMinConfidence(0.6)
    val model = fpgrowth.fit(dataset)

    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset).show()

    //save model
    model.save("/tmp/fp.ml")

    //load saved model
    import org.apache.spark.ml.fpm.FPGrowthModel
    val savedModel = FPGrowthModel.load("/tmp/fp.ml")
  }

}
