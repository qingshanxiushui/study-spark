package com.spark.stream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.HashMap

// 参考https://dblab.xmu.edu.cn/blog/1358/
object KafkaWordProducer {
  def main(args: Array[String]) {

    val brokers = "localhost:9092" //Kafka的broker的地址
    val topic = "test" //topic的名称
    val messagesPerSec = 3 //每秒发送3条消息
    val wordsPerMessage = 5 //每条消息包含5个单词（实际上就是5个整数）
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    // Send some messages
    while (true) {
      (1 to messagesPerSec).foreach { messageNum =>
        val str = (1 to wordsPerMessage).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")
        print(str)
        println()
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
    }
  }
}
