package com.atalibaba.ordertimeoutdetect

import java.util

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * CEP实现
  */
object OrderTimeoutNew {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val resourcePath = getClass.getResource("/OrderLog.csv")

        val dataStream = env.readTextFile(resourcePath.getPath)

            // val dataStream: DataStream[OrderEvent] = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\OrderTimeoutDetect\\src\\main\\resources\\OrderLog.csv")
            .map(data => {
            val dataArray = data.split(",")
            OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
        })
            //事件戳有序  不用找水位线延时
            .assignAscendingTimestamps(_.eventTime * 1000)

        //定义一个带事件限制的pattern  模式匹配 先选出创建订单后又支付的事件流
        val orderPayPattern = Pattern.begin[OrderEvent]("begin")
            .where(_.eventType == "create")
            .followedBy("follow")
            .where(_.eventType == "pay")
            .within(Time.seconds(15))

        //把模式应应用到stream上  得到一个patternStream
        val patternStream = CEP.pattern(dataStream, orderPayPattern)
        //4 调用select方法  提取事件序列 超时的事件要做报警
        val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
        val resultStrem = patternStream.select(orderTimeoutOutputTag,
            new OrderTimeoutSelect(),
            new OrderPaySelect())


        resultStrem.print("payed")
        resultStrem.getSideOutput(orderTimeoutOutputTag).print("timeout")

        env.execute("order time out")

        //定义一个输出标签  用来标明侧输出流


    }
}


//自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
        val timeoutOrderId = map.get("begin").iterator().next().orderId
        OrderResult(timeoutOrderId, "timeout")

    }
}

//自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
        val payedOrderId = map.get("follow").iterator().next().orderId
        OrderResult(payedOrderId, "payed successfully")

    }
}