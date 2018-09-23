package com.acervera.poc.sparkcircularreference

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CircularDependenciesApp {



  def run(implicit spark: SparkSession) = {
    import spark.implicits._

    val trees = Seq(Tree(1, Root(0, List(Branch(2, List.empty), Branch(3, List(Branch(4, List.empty)))))))
    val ds = spark.createDataset(trees)
    ds.show
  }


  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CircularReference App")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    run(spark)
  }

}
