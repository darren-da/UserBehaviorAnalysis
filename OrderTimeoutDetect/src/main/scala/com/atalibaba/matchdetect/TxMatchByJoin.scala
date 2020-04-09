package com.atalibaba.matchdetect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @author :YuFada
  * @date ： 2020/4/9 0009 上午 9:01
  *       Description：
  *
  *       双流join进行对账
  *       //两条流进行join处理 (join方法的缺陷 只能匹配上符合规则的，想和connect一样有侧输出流进行报警的话
  *       // 无法实现其功能)
  *
  *       一般情况的应用场景：（温度传感器）比如  当温度异常+烟雾报警了  ===============》此时  满足要求  则报警
  *
  *
  */
object TxMatchByJoin {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        //定义输入流  订订单事件流
        val resourcePath = getClass.getResource("/OrderLog.csv")

        val orderEventStream = env.readTextFile(resourcePath.getPath)
            //val orderEventStream = env.socketTextStream("hadoop102", 7777) //nc -lk 7777
            .map(data => {
            val dataArray = data.split(",")
            OrderEventWeb(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
        })
            .filter(_.txId != "")
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.txId)

        //读取支付到账事件流
        val receiptResource = getClass.getResource("/ReceiptLog.csv")
        val receiptEventStream = env.readTextFile(receiptResource.getPath)
            //val receiptEventStream = env.socketTextStream("hadoop102", 8888) //nc -lk 8888
            .map(data => {
            val dataArray = data.split(",")
            ReceptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
        })
            //一般情况下都以乱序数据的处理方式进行处理（实际生成数据流基本都以乱序流接收）
            .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[ReceptEvent]
            (Time.milliseconds(3000)) {
                override def extractTimestamp(element: ReceptEvent): Long = {
                    element.eventTime * 1000L
                }
            })
            .keyBy(_.txId)

        //两条流进行join处理 (join方法的缺陷 只能匹配上符合规则的，想和connect一样有侧输出流进行报警的话
        // 无法实现其功能)
        val processedStream = orderEventStream.intervalJoin(receiptEventStream)
            .between(Time.seconds(-5), Time.seconds(5))
            .process(new TxPayMatchByJoin)

        processedStream.print("matched:")
        env.execute("tx pay match by join job")
    }

    class TxPayMatchByJoin extends ProcessJoinFunction[OrderEventWeb, ReceptEvent, (OrderEventWeb, ReceptEvent)] {
        override def processElement(in1: OrderEventWeb, in2: ReceptEvent, context: ProcessJoinFunction[OrderEventWeb, ReceptEvent, (OrderEventWeb, ReceptEvent)]#Context, out: Collector[(OrderEventWeb, ReceptEvent)]): Unit = {

            //out.collect((left,right))

            out.collect((in1, in2)) //直接输出

        }
    }

}
