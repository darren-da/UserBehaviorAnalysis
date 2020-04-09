package com.atalibaba.ordertimeoutdetect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author :YuFada
  * @date ： 2020/4/3 0003 下午 19:40
  *       Description：
  *       目前只考虑一个  create  一个pay
  *
  *       缺点和存在问题：
  *       当创建订单后  时间还不到15分钟  就有支付成功  此时不会监控 订单支付成功  得到15分钟  事件才会被触发
  *
  *       改进：
  *       把正常匹配成功的数据放到主流中
  *       不正常匹配放到侧输出流中
  */
object OrderTimeOutWithoutCep {
    //定义侧输出流
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("ordertimeout")

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val resourcePath = getClass.getResource("/OrderLog.csv")
        // val orderEventStream: DataStream[String] = env.socketTextStream("hadoop102",7777)

        val orderEventStream = env.readTextFile(resourcePath.getPath)

            // val dataStream: DataStream[OrderEvent] = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\OrderTimeoutDetect\\src\\main\\resources\\OrderLog.csv")
            .map(data => {
            val dataArray = data.split(",")
            OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
        })
            //事件戳有序  不用找水位线延时
            .assignAscendingTimestamps(_.eventTime * 1000L)
            .keyBy(_.orderId)

        //定义 process function 进行超时检测
        //val timeOutWarningStream = orderEventStream.process(new OrderTimeoutWarning())
        val orderResultStream = orderEventStream.process(new OrderPayMatch())


        //timeOutWarningStream.print()
        orderResultStream.print("payed")
        orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

        env.execute("order timeout without cep job")
    }

    /**
      * 改进：
      * *       把正常匹配成功的数据放到主流中
      * *       不正常匹配放到侧输出流中
      */

    class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
        lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

        //保存定时器的时间戳为状态
        lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))


        override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
            //先读取状态
            val isPayed = isPayedState.value()
            val timerTs = timerState.value()
            //根据事件的类型进行分类判断  ，做不同的处理逻辑
            if (value.eventType == "create") {
                //1 如果是create事件，接下来判断pay是否来过
                if (isPayed) {
                    //1.1 如果已经pay过，匹配成功，输出主流  清空 状态
                    collector.collect(OrderResult(value.orderId, "payed successfully"))
                    context.timerService().deleteEventTimeTimer(timerTs)
                    timerState.clear()

                } else {
                    //1.2 如果没有pay过  注册定时器  等待pay的到来
                    val ts = value.eventTime * 1000L + 15 * 60 * 1000L
                    timerState.update(ts)

                }
            } else if (value.eventType == "pay") {
                //2 如果是pay事件先来，判断是否create过，用timer表示
                if (timerTs > 0) {
                    //2.1 如果有定时器，说明已经有create过
                    //继续判断 ，是否超过了timeout时间
                    if (timerTs > value.eventTime * 1000L) {
                        // 2.1.1 如果定时器时间还没到，那么输出成功匹配
                        collector.collect(OrderResult(value.orderId, "payed successfully"))

                    } else {
                        // 2.1.2 如果当前 payd的时间已经超时，那么输出到侧输出流
                        context.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
                    }
                    //输出结束，清空状态
                    context.timerService().deleteEventTimeTimer(timerTs)
                    timerState.clear()
                    isPayedState.clear()

                } else {
                    // 2.2 pay先到了 更新状态，注册定时器 等待create  (pay 先来  create还没来  乱序问题导致)
                    isPayedState.update(true)
                    context.timerService().registerEventTimeTimer(value.eventTime * 100L)
                    timerState.update(value.eventTime * 1000L)

                }
            }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
            if (isPayedState.value()) {
                //如果为真  表示pay先到  没等到create
                ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))

            } else {
                //表示 create到了，没等到pay
                ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
            }
            isPayedState.clear()
            timerState.clear()
        }

    }

    /**
      * 缺点和存在问题：
      * *       当创建订单后  时间还不到15分钟  就有支付成功  此时不会监控 订单支付成功  达到15分钟  事件才会被触发
      * *
      */
    //实现自定义的处理函数 （此方法对实际情况不是很合理）
    class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
        //保存pay支付事件是否来过的转态
        lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

        override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
            //先取出状态标志位
            val isPayed = isPayedState.value()

            if (value.eventType == "create" && !isPayed) {
                //如果遇到了create事件 ，并且pay没有来过，注册定时器开始等待 15分钟的定时
                context.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
            } else if (value.eventType == "pay") {
                //如果是pay事件 直接把状态改为 true
                isPayedState.update(true)

            }
        }

        //目前逻辑处理完了   还需要定时器进行触发
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
            //判断isPayed是否为true
            val ispayed = isPayedState.value()
            if (ispayed) {
                out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
            } else {
                out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
            }

            //清空状态

            isPayedState.clear()
        }
    }

}


