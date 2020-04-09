package com.atalibaba

import com.atalibaba.hotitemanalys.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author :YuFada
  * @date ： 2020/3/31 0031 下午 19:09
  *       Description：
  */
//另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）
//定义输出 样例类
case class UvCount(
                      windowEnd: Long,
                      uvCount: Long
                  )

object UniqueVisitor {
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
            .assignAscendingTimestamps(_.timestamp * 1000)
            .filter(_.behavior == "pv") //元素的behavior等于pv
            .timeWindowAll(Time.hours(1)) //把所有元数收集起来  然后再 window里进行set去重
            .apply(new UvCountByWindow())

        stream.print("UV")

        env.execute("UV job")

    }


    class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
        override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
            //定义一个 scala set， 用于保存数据并去重 set存在内存中
            var idSet = Set[Long]()  //如果内存不够 可以放到redis中去去重
            //把当前窗口所有数据的id收集到Set中，最后输出set的大小
            for (userBehavior <- input){
               idSet +=userBehavior.userId

            }
            out.collect(UvCount(window.getEnd,idSet.size))

        }
    }

}
