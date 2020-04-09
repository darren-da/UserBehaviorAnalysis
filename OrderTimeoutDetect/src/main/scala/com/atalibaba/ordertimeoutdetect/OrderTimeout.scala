package com.atalibaba.ordertimeoutdetect

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author :YuFada
  * @date ： 2020/3/27 0027 下午 15:45
  *       Description：
  *
  *       CEP实现
  */

//定义样例类
//输入订单事件
case class OrderEvent(
                         orderId: Long,
                         eventType: String,
                         eventTime: Long
                     )

//输出订单处理结果数据
case class OrderResult(
                          orderId: Long,
                          resultMsg: String
                      )

object OrderTimeout {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val  resourcePath=getClass.getResource("/OrderLog.csv")

        val dataStream=env.readTextFile(resourcePath.getPath)

       // val dataStream: DataStream[OrderEvent] = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\OrderTimeoutDetect\\src\\main\\resources\\OrderLog.csv")
            .map(data => {
                val dataArray = data.split(",")
                OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
            })
            //事件戳有序  不用找水位线延时
            .assignAscendingTimestamps(_.eventTime * 1000)
            .keyBy(_.orderId)

        //定义一个带事件限制的pattern 先选出创建订单后又支付的事件流
        val orderPayPattern = Pattern.begin[OrderEvent]("begin")
            .where(_.eventType == "create")
            .followedBy("follow")
            .where(_.eventType == "pay")
            .within(Time.seconds(15))

        //定义一个输出标签  用来标明侧输出流

        val orderTimeOutputTag = OutputTag[OrderResult]("orderTimeOut")

        //将pattern作用到input Stream上，得到一个pattern stream

        val patternStream = CEP.pattern(dataStream.keyBy(_.orderId), orderPayPattern)
        //调用select 得到最后复合输出流
        import scala.collection.Map
        val complexResult: DataStream[OrderResult] = patternStream.select(orderTimeOutputTag)(

            //实现pattern timeout function
            (orderPayEvents: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
                val timeoutOrderId = orderPayEvents.getOrElse("begin", null).iterator.next().orderId
                OrderResult(timeoutOrderId, "order time out")
            }
        )(
            // pattern select function
            (orderPayEvents: Map[String, Iterable[OrderEvent]]) => {
                val payedOrderId = orderPayEvents.getOrElse("follow", null).iterator.next().orderId
                OrderResult(payedOrderId, "order payed successfully")
            }
        )

        //sink
        //已经正常支出的数据流
        complexResult.print("payed")
        //从符合输出流里拿到侧输出流
        val timeoutResult = complexResult.getSideOutput(orderTimeOutputTag)

        timeoutResult.print("timesout")

        env.execute("OrderTimeOutDetect")
    }

}
