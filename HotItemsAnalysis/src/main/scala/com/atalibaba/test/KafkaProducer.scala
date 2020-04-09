package com.atalibaba.test

import java.sql.Time
import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @author :YuFada
  * @date ： 2020/3/20 0020 下午 14:16
  *       Description：
  */
object KafkaProducer {
    def main(args: Array[String]): Unit = {

        //更换数据源  kafka consumer
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop102:9092")
        properties.setProperty("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("auto.offset.reset", "latest")

      val bufferedSource=io.Source.fromFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        val producer=new KafkaProducer[String,String](properties)

        for (line<-bufferedSource.getLines()){
            val record=new ProducerRecord[String,String]("hotitems",line)

            println("one record is sending..............")
            producer.send(record)
        }

        producer.close()

    }

}
