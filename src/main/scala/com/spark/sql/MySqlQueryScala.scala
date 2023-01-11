package com.spark.sql
import org.apache.spark.sql.SparkSession
object MySqlQueryScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MySqlQueryScala").master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test_spark?useUnicode=true&characterEncoding=utf-8")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "123456")
      .load()
    jdbcDF.show()
  }
}
