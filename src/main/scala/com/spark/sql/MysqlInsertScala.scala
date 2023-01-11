package com.spark.sql

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object MysqlInsertScala {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("MySqlQueryScala").master("local").getOrCreate()
      val studentRDD =  spark.sparkContext.parallelize(Array("7 Zhangsan M 12", "8 Lisi F 13")).map(_.split(" ")) //设置两条数据表示两个学生信息
      val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true))) //设置模式信息
      val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt)) //下面创建Row对象，每个Row对象都是rowRDD中的一行
      val studentDF = spark.createDataFrame(rowRDD, schema) //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
      //下面创建一个prop变量用来保存JDBC连接参数
      val prop = new Properties()
      prop.put("user", "root") //表示用户名是root
      prop.put("password", "123456") //表示密码是hadoop
      prop.put("driver", "com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver
      //下面就可以连接数据库，采用append模式，表示追加记录到数据库spark的student表中
      studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test_spark", "test_spark.student", prop)

      /*val conf = new SparkConf().setAppName("MysqlInsertScala").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val studentRDD = sc.parallelize(Array("7 Zhangsan M 12", "8 Lisi F 13")).map(_.split(" ")) //设置两条数据表示两个学生信息
      val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true))) //设置模式信息
      val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt)) //下面创建Row对象，每个Row对象都是rowRDD中的一行
      val studentDF = sqlContext.createDataFrame(rowRDD, schema) //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
      //下面创建一个prop变量用来保存JDBC连接参数
      val prop = new Properties()
      prop.put("user", "root") //表示用户名是root
      prop.put("password", "123456") //表示密码是hadoop
      prop.put("driver", "com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver
      //下面就可以连接数据库，采用append模式，表示追加记录到数据库spark的student表中
      studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test_spark", "test_spark.student", prop)
    */
    }
}
