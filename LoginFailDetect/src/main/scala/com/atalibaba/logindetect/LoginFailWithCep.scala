package com.atalibaba.logindetect


import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author :YuFada
  * @date ： 2020/3/25 0025 下午 22:16
  *       Description：
  */
object LoginFailWithCep {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val dataStream=env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
                .map(data=>{
                    val dataArray=data.split(",")
                    LoginEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).toLong)
                })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
                override def extractTimestamp(element: LoginEvent): Long = {
                   element.eventTime*1000
                }
            })

        //定义pattern  对事件流进行模式匹配

        val loginFailPattern=Pattern.begin[LoginEvent]("begin")
                .where(_.eventType=="fail")
                .next("next")
                .where(_.eventType=="fail")
                .within(Time.seconds(2))
        //在输入的流的基础上应用pattern 得到匹配的Pattern stream
        val patternStream=CEP.pattern(dataStream.keyBy(_.userId),loginFailPattern)

        //用select方法从pattern stream中提取出数据

     /*  import scala.collection.Map
      val loginFailDataStream:DataStream[Warning]=patternStream.select((patterEvents :Map[String,Iterable[LoginEvent]])=>{

          //从map中取出对应的登录失败事件 包装成Warning输出
          val firstFailEvent=patterEvents.getOrElse("begin",null).iterator.next()
          val secondFailEvent=patterEvents.getOrElse("next",null).iterator.next()

          Warning(firstFailEvent.userId,firstFailEvent.eventTime,secondFailEvent.eventTime,"logsin fail warning")


      })*/

        //写法二
        val loginFailDataStream=patternStream.select(new MySelectFunction())
        //得到的警告信息输出 sink
        loginFailDataStream.print("warning")

        env.execute("loginFailWithCep")
    }

}
class MySelectFunction() extends PatternSelectFunction[LoginEvent,Warning] {
    override def select(patternEvents: util.Map[String, util.List[LoginEvent]]): Warning = {
        //从map中取出对应的登录失败事件 包装成Warning输出
        val firstFailEvent = patternEvents.getOrDefault("begin", null).iterator.next()
        val secondFailEvent = patternEvents.getOrDefault("next", null).iterator.next()
        Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "logsin fail warning")
    }
}
