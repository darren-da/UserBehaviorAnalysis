package com.atalibaba.logindetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @author :YuFada
  * @date ： 2020/3/25 0025 下午 13:31
  *       Description：
  */

//case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
object LoginFail2 {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val loginEventStream = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
            .map( data => {
                val dataArray = data.split(",")
                LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
            })
            .assignTimestampsAndWatermarks(new
                    BoundedOutOfOrdernessTimestampExtractor[LoginEvent]
                    (Time.milliseconds(3000)) {
                override def extractTimestamp(element: LoginEvent): Long ={
                    element.eventTime * 1000L
                }
            })
            .keyBy(_.userId)
            .process(new MatchFunction2())
            .print()

        env.execute("Login Fail Detect Job")
    }

    class MatchFunction2 extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {

        // 定义状态变量
        lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
            new ListStateDescriptor[LoginEvent]("saved login", classOf[LoginEvent]))

        override def processElement(login: LoginEvent,
                                    context: KeyedProcessFunction[Long, LoginEvent,
                                        LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {

            if (login.eventType == "fail") {
                loginState.add(login)
            }
            // 注册定时器，触发事件设定为2秒后
            context.timerService.registerEventTimeTimer(login.eventTime * 1000 + 2 * 1000)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Long, LoginEvent,
                                 LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {


            val allLogins: ListBuffer[LoginEvent] = ListBuffer()
            import scala.collection.JavaConversions._
            for (login <- loginState.get) {
                allLogins += login
            }
            loginState.clear()

            if (allLogins.length > 1) {
                out.collect(allLogins.head)
            }
        }
    }
}

