package com.atalibaba.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author :YuFada
  * @date ： 2020/4/1 0001 下午 22:48
  *       Description：
  *       不分渠道的市场推广统计
  */
object AppMarketing {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val dataStream = env.addSource(new SimulatedEventSource())
            .assignAscendingTimestamps(_.timestamp)
            .filter(_.behavior != "UNINSTALL")
            .map(data => {
                ("dummyKey", 1L) //后面给个1   最后的count值
            })
            .keyBy(_._1) //以渠道和行为作为key进行分组
            .timeWindow(Time.hours(1), Time.seconds(10))
            .aggregate(new CountAgg(), new MarktingCountTotal())


        dataStream.print()
        env.execute("app marketing job")

    }

}

class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: (String, Long), acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//WindowFunction[IN, OUT, KEY, W]
class MarktingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
        val startTs = new Timestamp(window.getStart).toString
        var endTs = new Timestamp(window.getEnd).toString
        val count = input.iterator.next()
        out.collect(MarketingViewCount(startTs, endTs, "app marketing", "total", count))
    }
}