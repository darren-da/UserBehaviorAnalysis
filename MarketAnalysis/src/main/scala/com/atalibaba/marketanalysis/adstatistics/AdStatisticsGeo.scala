package com.atalibaba.marketanalysis.adstatistics



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
  * @date ： 2020/4/2 0002 上午 10:32
  *       Description：
  *       市场分析之广告统计
  *       未进行过滤操作
  */

//定义输出的广告点击事件的样例类
case class AdClickEvent(
                           userId: Long,
                           adId: Long,
                           province: String,
                           city: String,
                           timestamp: Long
                       )

//按照省份统计的输出结果案例类
case class CountByProvince(
                              windowEnd: String,
                              provice: String,
                              count: Long
                          )

//输出的黑名单报警信息
case class BlackListWarning(
                           userId:Long,
                           addId:Long,
                           msg:String
                           )

object AdStatisticsGeo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val resourcePath = getClass.getResource("/AdClickLog.csv")
        val adEventStream = env.readTextFile(resourcePath.getPath)
            .map(data => {
                val dataArray = data.split(",")
                AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)
        //根据省份做分组 ，开窗聚合
        val adCountStream=adEventStream
            .keyBy(_.province)
            .timeWindow(Time.hours(1),Time.seconds(5))
            .aggregate(new AdCountAgg(),new AdCountResult() )

        adCountStream.print()
        env.execute("ad statistics job")
    }

}

//自定义聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: AdClickEvent, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
}


//自定义窗口处理函数
class AdCountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
        out.collect(CountByProvince(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
    }
}