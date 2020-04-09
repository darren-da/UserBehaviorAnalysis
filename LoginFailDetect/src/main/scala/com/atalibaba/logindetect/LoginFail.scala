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
  * @date ： 2020/3/25 0025 上午 11:03
  *       Description：
  */

//定义样例类  根据数据具体细节而定
//输入的登录时间流
case class LoginEvent(
                         userId: Long,
                         ip: String,
                         eventType: String,
                         eventTime: Long
                     )

//输出的报警信息
case class Warning(
                      userId: Long,
                      firstFailTime: Long,
                      lastFailTime: Long,
                      warningMsg: String
                  )

object LoginFail {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //自定义测试数据源
        val loginStream = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
            .map(line => {
                val lineArray = line.split(",")
                LoginEvent(lineArray(0).trim.toLong, lineArray(1).trim, lineArray(2).trim, lineArray(3).trim.toLong)
            })
            /*  //有序数据这么操作
              .assignAscendingTimestamps(_.eventTime*1000)*/

            //乱序数据分配时间戳和水印(水印时间根据具体数据而定)
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
            override def extractTimestamp(element: LoginEvent): Long = {
                element.eventTime * 1000L
            }
        })
            .keyBy(_.userId) //根据用户Id做分组
            .process(new MatchFunction())
            .print()
        //sink 打印输出
        env.execute("loginFailDetect")

    }

}

// K I  O
class MatchFunction() extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    //状态变成  懒加载
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))
    //
    override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

        if (value.eventType == "fail") {
            loginState.add(value)
            context.timerService().registerEventTimeTimer((value.eventTime + 2) * 1000)
        }

        else
            loginState.clear()
    }

    //先onTime
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {

        val allLogins: ListBuffer[LoginEvent] = ListBuffer()

        val iter = loginState.get().iterator()
        while (iter.hasNext)
            allLogins += iter.next()

            loginState.clear()
            //如果长度大于1  说明有两个以上的登录失败事件  输出报警信息
            if (allLogins.length > 1) {

                out.collect(Warning(allLogins.head.userId,
                    allLogins.head.eventTime,
                    allLogins.last.eventTime,
                    "login fail in 2 seconds for:" + allLogins.length + "times"))
            }
        }

}

