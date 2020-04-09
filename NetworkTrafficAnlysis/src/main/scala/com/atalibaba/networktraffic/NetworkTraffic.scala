package com.atalibaba.networktraffic

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @author :YuFada
  * @date ： 2020/3/24 0024 上午 9:22
  *       Description：
  */

//  输入web log数据流 样例类
case class ApacheLogEvent(
                             ip: String,
                             userName: String,
                             eventTime: Long,
                             method: String,
                             url: String
                         )

//中间统计数量的数据类型 样例类
case class UrlViewCount(
                           url: String,
                           windowEnd: Long,
                           count: Long
                       )

object NetworkTraffic {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        import org.apache.flink.streaming.api.scala._
        val dataStream = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\NetworkTrafficAnlysis\\src\\main\\resources\\apache.log")
            .map(data => {
                val dataArray = data.split(" ")
                //把Log时间转换为时间戳
                val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val timestamp: Long = simpleDataFormat.parse(dataArray(3)).getTime
                ApacheLogEvent(dataArray(0), dataArray(2), timestamp, dataArray(5), dataArray(6))
            }) //乱序数据处理并声明水位延时（延时根据数据本身情况设定）
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(40)) {
            override def extractTimestamp(element: ApacheLogEvent): Long = {
                //已经是毫秒
                element.eventTime

            }
        })

            .keyBy(_.url)
            .timeWindow(Time.minutes(1), Time.seconds(5)) //开滑动窗口
            .aggregate(new CountAgg(), new WindowResultFunction())
            .keyBy(_.windowEnd) //根据时间窗口进行分组
            .process(new TopNUrls(5))
            .print() //sink输出
        env.execute("Network traffic Analysis")

    }

}

//实现自定义的累加器
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class WindowResultFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {


    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
        val url: String = key
        val windowEnd: Long = window.getEnd
        val count: Long = input.iterator.next()
        out.collect(UrlViewCount(url, windowEnd, count))
    }
}

    class TopNUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

        //懒加载的方式定义state  不用写 open了
        lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

        override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
            urlState.add(value)
            //注册定时器 ，当定时器触发时，应该收集到了所有数据
            context.timerService().registerEventTimeTimer(value.windowEnd + 100)
        }


        //实现ontimer 定时的方法
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

            //从state里获取所有的数据
            val allUrlViewCounts: ListBuffer[UrlViewCount] = ListBuffer()
            //
            //        import scala.collection.JavaConversions._
            //
            //        for (urlViewCount <- urlStae.get()) {
            //            allUrlViewCounts += urlViewCount
            //        }

            val iter = urlState.get().iterator()
            while (iter.hasNext)
                allUrlViewCounts += iter.next()
            urlState.clear() //一定别忘清空

            val sortedUrlViewCounts = allUrlViewCounts.sortWith(_.count > _.count).take(topSize)

            //把结果格式化string
            // 将排名信息格式化成 String, 便于打印
            val result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n")


            for (i <- sortedUrlViewCounts.indices) { // for (i<- 0 until sortedItems.length) 效果一样
                val currentUrlView: UrlViewCount = sortedUrlViewCounts(i)
                // e.g.  No1：  URL=/blogs/tags/frefox?/..  流量=55
                result.append("No").append(i + 1).append(":")
                    .append("  URL=").append(currentUrlView.url)
                    .append("  流量=").append(currentUrlView.count).append("\n")
            }
            result.append("====================================\n\n")
            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1500)
            out.collect(result.toString)

        }


    }



