package com.atalibaba.matchdetect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
  * @author :YuFada
  * @date ： 2020/4/3 0003 上午 10:27
  *       Description：
  *       实现实时对账 需求（从前端日志埋点事件点击付款，到后端得到响应，最后两条流进行融合分析）
  *
  *       分析：
  *       对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同
  *       的日志信息，所以我们要同时读入两条流的数据来做合并处理。
  *       利用connect将两条流进行连接，然后用自定义的CoProcessFunction进行处理。
  */
//前端完成支付的输入事件流
case class OrderEventWeb(
                            orderId: Long,
                            eventType: String,
                            txId: String, //交易id
                            eventTime: Long
                        )

//定义接收流事件的样例类
case class ReceptEvent(
                          txId: String, //交易id
                          payChannel: String, //支付方式渠道
                          eventTime: Long //事件时间
                      )

object TxMatchDetect {
    //异常没对上的账输出到侧输出流
    //定义侧输出流
    val unmatchedPays = new OutputTag[OrderEventWeb]("unmatchedPays")
    val unmatchedRecipts = new OutputTag[ReceptEvent]("unmatchedRecipts")


    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        //定义输入流  订订单事件流
        val resourcePath = getClass.getResource("/OrderLog.csv")

        //val orderEventStream = env.readTextFile(resourcePath.getPath)
        val orderEventStream = env.socketTextStream("hadoop102", 7777) //nc -lk 7777
            .map(data => {
                val dataArray = data.split(",")
                OrderEventWeb(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
            })
            .filter(_.txId != "")
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.txId)

        //读取支付到账事件流
        val receiptResource = getClass.getResource("/ReceiptLog.csv")
        //val receiptEventStream = env.readTextFile(receiptResource.getPath)
        val receiptEventStream = env.socketTextStream("hadoop102", 8888) //nc -lk 8888
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

        //将两条流连接起来共同处理
        val processedStream = orderEventStream.connect(receiptEventStream)
            .process(new txPayMatch())

        processedStream.print("matched")
        processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
        processedStream.getSideOutput(unmatchedRecipts).print("unmatchedRecipts")

        env.execute("tx match job")
    }

    class txPayMatch() extends CoProcessFunction[OrderEventWeb, ReceptEvent, (OrderEventWeb, ReceptEvent)] {
        //定义状态来保存已经到达的订单支付事件和到账事件
        lazy val payState: ValueState[OrderEventWeb] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEventWeb]("pay-state", classOf[OrderEventWeb]))
        lazy val recepitState: ValueState[ReceptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceptEvent]("recepit-state", classOf[ReceptEvent]))

        //订单支付事件数据的处理
        override def processElement1(pay: OrderEventWeb, context: CoProcessFunction[OrderEventWeb, ReceptEvent, (OrderEventWeb, ReceptEvent)]#Context, out: Collector[(OrderEventWeb, ReceptEvent)]): Unit = {
            //判断有没有对应的到账事件
            val receipt = recepitState.value()
            if (receipt != null) {
                //如果已有receipt 在主流输出匹配信息
                out.collect((pay, receipt))
                recepitState.clear()

            } else {
                //如果还没到 ，那么把pay存入状态 ，并且注册一个定时器
                payState.update(pay)
                context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
            }

        }

        //到账事件的处理
        override def processElement2(receipt: ReceptEvent, context: CoProcessFunction[OrderEventWeb, ReceptEvent, (OrderEventWeb, ReceptEvent)]#Context, out: Collector[(OrderEventWeb, ReceptEvent)]): Unit = {
            //同样的处理流程
            val pay = payState.value()
            if (pay != null) {
                //如果有pay  主输出流输出匹配信息
                out.collect((pay, receipt))
                payState.clear()
            } else {
                recepitState.update(receipt)
                //如果还没到 ，那么把receipt存入状态 ，并且注册一个定时器
                context.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)

            }
        }

        //定时器触发 输出报警
        override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEventWeb, ReceptEvent, (OrderEventWeb, ReceptEvent)]#OnTimerContext, out: Collector[(OrderEventWeb, ReceptEvent)]): Unit = {
            //时间到了，入股偶还没有收到某个事件，那么输出报警信息
            if (payState.value() != null) {
                //receipt没来，输出pay到侧输出流
                ctx.output(unmatchedPays, payState.value())
            }
            if (recepitState.value() != null) {
                //pay 没来，输出receipt到侧输出流
                ctx.output(unmatchedRecipts, recepitState.value())
            }
            //清空操作
            payState.clear()
            recepitState.clear()
        }
    }

}


