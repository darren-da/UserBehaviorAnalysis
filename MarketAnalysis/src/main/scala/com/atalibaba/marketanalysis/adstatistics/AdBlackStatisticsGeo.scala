package com.atalibaba.marketanalysis.adstatistics

import java.sql.Timestamp

import com.atalibaba.marketanalysis.adstatistics.AdBlackStatisticsGeo.blackListOutputTag
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author :YuFada
  * @date ： 2020/4/2 0002 上午 11:28
  *       Description：
  *       市场分析之广告统计
  *       进行过滤操作  并将频繁点击用户设置为黑名单用户
  */
object AdBlackStatisticsGeo {
    //定义侧输出流的Tag
    val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

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
        //自定义 process function 过滤大量刷点击行为
        val filterBlackListStream = adEventStream
            .keyBy(data => (data.userId, data.adId))
            .process(new FilterBlackListUser(100))

        //根据省份做分组 ，开窗聚合
        val adCountStream = filterBlackListStream
            .keyBy(_.province)
            .timeWindow(Time.hours(1), Time.seconds(5))
            .aggregate(new AdCountAgg2(), new AdCountResult2())

        adCountStream.print("count")
        filterBlackListStream.getSideOutput(blackListOutputTag).print("black list")
        env.execute("ad statistics job")
    }


}

class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    //定义状态  ，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    //保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetImme-state", classOf[Long]))

    override def processElement(value: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
        //取出count 状态
        val curCount = countState.value()
        //如果是第一次处理，注册触发器 每天零点触发
        if (curCount == 0) {
            val ts = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
            resetTimer.update(ts)
            context.timerService().registerProcessingTimeTimer(ts)
            //判断计数是否达到上限  如果达到加入黑名单
        }
        if (curCount >= maxCount) {
            //判断是否发送过黑名单  只发送一次
            if (!isSentBlackList.value()) {
                isSentBlackList.update(true)
                //输出到侧输出流
                context.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over" + maxCount + "times today"))
            }
            return
        }
        //输出数据到主流 计数状态+1
        countState.update(curCount + 1)
        out.collect(value)

    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
        //定时器触发时 清空状态
        if (timestamp == resetTimer.value()) {
            isSentBlackList.clear()
            countState.clear()
            resetTimer.clear()
        }
    }

}


//自定义聚合函数
class AdCountAgg2() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: AdClickEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


//自定义窗口处理函数
class AdCountResult2() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
        out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
    }
}