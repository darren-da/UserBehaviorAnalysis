package com.atalibaba

import com.atalibaba.hotitemanalys.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @author :YuFada
  * @date ： 2020/4/1 0001 下午 18:35
  *       Description：
  */
//布隆过滤器（Bloom Filter）
object UvWithBloom {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        //读取文件路径 用相对路径定义数据源（更常用的方法）<本地测试没有问题，但提交运行就出错了>
        val resourcePath = getClass.getResource("/UserBehavior.csv")
        val stream = env.readTextFile(resourcePath.getPath)
            //        val stream = env.readTextFile("D:\\DevelopWorkspace\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
            .map(data => {
            val dataArray = data.split(",")
            UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
        })
            .assignAscendingTimestamps(_.timestamp * 1000L)
            .filter(_.behavior == "pv") //元素的behavior等于pv
            .map(data => ("dummyKey", data.userId)
        )
            .keyBy(_._1)
            .timeWindow(Time.hours(1))
            .trigger(new MyTrigger())
            .process(new UvCountWithBloom())

        stream.print()
        env.execute("uv with bloom job")

    }
}

//自定义窗口触发器  二元组
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        //每来一条数据 就 直接出发窗口操作  并清空所有窗口状态
        TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    ]

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

    }
}

//定义一个布隆过滤器  几十G的数据 Int类型就可以扛得住
class Bloom(size: Long) extends Serializable {
    //位图的总大小
    private val cap = if (size > 0) size else 1 << 27 //相当于能存16M的位图    位运算

    //定义hash函数  随机数种子 seed

    def hash(value: String, seed: Int): Long = {
        var result: Long = 0L

        for (i <- 0 until value.length) {
            result = result * seed + value.charAt(i)

        }
        //最后返回只取27位 按位的与运算
        result & (cap - 1)
    }
}

//实现自定义的 procesFunction
//从上面定义的  输入是 一个二元组(String,Long) 输出是 UvCount I O K Window
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    //定义与redis的连接
    lazy val jedis = new Jedis("hadoop102", 6379)
    lazy val bloom = new Bloom(1 << 29) //定义64M的一个位图的key  可以处理 512M（5亿多的key）
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
        //位图的存储方式，key是以windowEnd，value是bitmap（位图）
        val storeKey = context.window.getEnd.toString
        var count = 0L
        //把每个窗口的uv count值 也存入名为count的redis表中，存放内容为（windowEnd>uvCount）所以要先从redis中读取
        if (jedis.hget("count", storeKey) != null) {
            count = jedis.hget("count", storeKey).toLong
        }
        //用布隆过滤器判断当前用户是否存在
        val userId = elements.last._2.toString
        //算hash 到位图里去看对应的位置去看0还是1  随机数种子给的 61  一般给的是随机数（质数）分布的散开一些《在允许范围给的大一点比较好》
        val offset = bloom.hash(userId, 61)
        //定义一个标志位 ，判断rdis位图中有没有这一位
        val isExit = jedis.getbit(storeKey, offset)

        //可以隔一段时间再和redis交互一次，这样可节省很多时间
        if (!isExit) {
            //如果不存在，微乳对应的位 置1，count+1
            jedis.setbit(storeKey, offset, true)
            jedis.hset("count", storeKey, (count + 1).toString)
            out.collect(UvCount(storeKey.toLong,count+1))
        } else {
            out.collect(UvCount(storeKey.toLong,count))
        }

    }
}