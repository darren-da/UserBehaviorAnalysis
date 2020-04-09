package com.atalibaba.logindetect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author :YuFada
  * @date ： 2020/3/25 0025 上午 11:03
  *       Description：
  */

//定义样例类  根据数据具体细节而定
//输入的登录时间流
//case class LoginEvent(
//                         userId: Long,
//                         ip: String,
//                         eventType: String,
//                         eventTime: Long
//                     )
//
////输出的报警信息
//case class Warning(
//                      userId: Long,
//                      firstFailTime: Long,
//                      lastFailTime: Long,
//                      warningMsg: String
//                  )

object LoginFail_new {
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
            .process(new MathcFunction_new())
            .print()





        //sink 打印输出


        env.execute("loginFailDetect")

    }

}

// K I  O
class MathcFunction_new() extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    //状态变成  懒加载
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))


    /*  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

          if (value.eventType == "fail") {
              loginState.add(value)
              context.timerService().registerEventTimeTimer(value.eventTime * 1000 + 2 * 1000)
          }
          else
              loginState.clear()
      }

      //先onTime  根据实际情况  定时器不符合逻辑
      /*
      说明如下：
      当两个登录失败的请求   在还没达到两秒的时候就两次登录失败
      因此需报警  而不是等到2秒了再判断2秒内是否两次登录失败
       */
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {

          val allLogins: ListBuffer[LoginEvent] = ListBuffer()

          val iter = loginState.get().iterator()
          while (iter.hasNext) {
              allLogins += iter.next()
          }
              loginState.clear()
              //如果长度大于1  说明有两个以上的登录失败事件  输出报警信息
              if (allLogins.length > 1) {


                  out.collect(Warning(allLogins.head.userId,
                      allLogins.head.eventTime,
                      allLogins.last.eventTime,
                      "login fail in 2 seconds for:" + allLogins.length + "times"
                  ))
              }
          }*/
    override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
        //首先按照type做筛选  ，如果success 直接清空，如果是fail再做处理

        if (value.eventType == "fail") {
            //如果已经有登录失败的数据 就判断是否在2秒内

            val iter = loginState.get().iterator()
            if (iter.hasNext) {
                val firstFail = iter.next()
                //如果两次登录失败的间隔小于2 s  输出报警
                if (value.eventType < firstFail.eventType + 2) {
                    out.collect(Warning(value.userId,
                        firstFail.eventTime,
                        value.eventTime,
                        "login fail in 2 seconds"))
                }
                //把最近一次登录失败的数据  更新写入 state中
                //                loginState.clear()
                //                loginState.add(value)

                val failList = new util.ArrayList[LoginEvent]()
                failList.add(value)
                loginState.update(failList)

            } else {
                //如果 state中没有登录失败的数据，就直接添加进去
                loginState.add(value)
            }
        } else {
            loginState.clear()
        }
    }
}

