package com.acervera.poc.sparkcircularreference

import org.apache.spark.SparkConf
import org.apache.spark.custom.udts.BranchUDType
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class CircularDependenciesTest extends FunSuite  {
  test("test sql"){

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CircularReference Test")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    BranchUDType.register()

    val treesOriginal = Seq(
      Tree(1, Root(0, List(Branch(2, List.empty), Branch(3, List(Branch(4, List(Branch(5, List.empty)))))))),
      Tree(1, Root(0, List.empty))
    )

    val ds = spark.createDataset(treesOriginal)
    ds.printSchema()
    ds.show(false)

    ds
      .select(
        col("id"),
        col("root.id").as("rootId"),
        size(col("root.branches"))
      )
      .where("size(root.branches) > 0")
      .show(false)

  }
}
