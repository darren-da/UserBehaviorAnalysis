package com.atalibaba.hotitemanalys
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @author :YuFada
  * @date ： 2020/3/19 0019 上午 10:24
  *       Description：
  */

//输入数据样例类
case class UserBehavior(userId: Long, iteamId: Long, categoryId: Int, behavior: String, timestamp: Long)

//中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
    def main(args: Array[String]): Unit = {

        //更换数据源  kafka consumer
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop102:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")


        //创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //指定并行度
        env.setParallelism(1)
import org.apache.flink.streaming.api.scala._
        //指定Time类型为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
       // val dataStream = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

            val dataStream=env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))

            .map(line => {
                val lineArray = line.split(",")
                UserBehavior(lineArray(0).trim.toLong, lineArray(1).trim.toLong,
                    lineArray(2).trim.toInt, lineArray(3).trim, lineArray(4).trim.toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000)
            .filter(_.behavior == "pv") //过滤出点击浏览事件
            //.keyBy("itemId") //按照itemId分区  分成不同的支流
            .keyBy(_.iteamId)
            // .window(SlidingEventTimeWindows.of(Time.minutes(60),Time.minutes(5))) //上面是下面的简写
            .timeWindow(Time.minutes(60), Time.minutes(5)) //开时间窗口 滑动窗口
            .aggregate(new CountAgg(), new WindowResultFunction())

            /*  目前已经到此步骤
      itemId      1              itemId      1
       windowEnd 10:00-11:00      windowEnd  10:05-11:05  。。。。。。。。
       count     4                count      3

       itemId      2             itemId       2
       windowEnd 10:00-11:00      windowEnd  10:05-11:05
       count     4                count      8*/

            .keyBy(_.windowEnd) //接下来按照windowEnd进行 分组

            // 排序  topN  想要的转换输出
            .process(new TopNHotItems(3))

        //sink输出
            .print("items")

        //调用executor执行任务
        env.execute("Hot Items")


    }

}


//自定义预聚合  累加器 来一条数据  +1
//Long  计数  Long 输出
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//窗口关闭的操作 包装成ItemViewCount
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        //val itemId=key.asInstanceOf[Tuple1[Long]].f0

        val itemId: Long = key
        val windowEnd: Long = window.getEnd
        val count: Long = input.iterator.next()
        out.collect(ItemViewCount(itemId, windowEnd, count))

    }
}

//求出TopN  并输出
class TopNHotItems(size: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    //来数据后存储到状态后端
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //异常考虑  获取当前运行环境中的  ListState  用来回复  iteamState
        val iteamStateDesc = new ListStateDescriptor[ItemViewCount]("iteam-state", classOf[ItemViewCount])
        itemState = getRuntimeContext.getListState(iteamStateDesc)
    }

    override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
        // 每一条数据的暂时存入iteamState中
        itemState.add(value)
        //注册一个定时器 延迟触发
        context.timerService().registerEventTimeTimer(value.windowEnd + 100)

    }

    //核心处理流程  在定时器触发时进行操作，可以认为之前的数据都已经到了
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        super.onTimer(timestamp, ctx, out)
        val allItems: ListBuffer[ItemViewCount] = ListBuffer()

        //里面可能涉及到java的转换  因此引入下面
        import scala.collection.JavaConversions._
        //拿出所有东西
        for (item <- itemState.get()) {
            allItems += item
        }

        //提前清除状态数据
        itemState.clear()

        // 按照点击量从大到小排序
        val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(size)
        // 将排名信息格式化成 String, 便于打印
        val result: StringBuilder = new StringBuilder
        result.append("====================================\n")
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")


        for (i <- sortedItems.indices) { // for (i<- 0 until sortedItems.length-1) 效果一样
            val currentItem: ItemViewCount = sortedItems(i)
            // e.g.  No1：  商品ID=12224  浏览量=2413
            result.append("No").append(i + 1).append(":")
                .append("  商品ID=").append(currentItem.itemId)
                .append("  浏览量=").append(currentItem.count).append("\n")
        }
        result.append("====================================\n\n")
        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1500)
        out.collect(result.toString)

    }


}