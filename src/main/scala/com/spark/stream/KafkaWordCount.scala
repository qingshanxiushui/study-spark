package com.spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 参考 https://blog.csdn.net/weixin_43975771/article/details/125952756
// 参考 https://blog.csdn.net/dkl12/article/details/80366134
object KafkaWordCount {
  def main(args: Array[String]) {
    val sc = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    //刷新时间设置为1秒
    val ssc = new StreamingContext(sc, Seconds(10))
    //设置检查点，如果存放在HDFS上面，则写成类似ssc.checkpoint("/user/hadoop/checkpoint")这种形式，但是，要启动hadoop
    //ssc.checkpoint("file:///usr/local/spark/mycode/kafka/checkpoint")

    //消费者配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("test") //消费主题，可以同时消费多个

    //创建DStream，返回接收到的输入数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val lines = stream.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val pair = words.map(x => (x, 1))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2) //这行代码的含义在下一节的窗口转换操作中会有介绍
    wordCounts.print
    ssc.start
    ssc.awaitTermination
  }
}
