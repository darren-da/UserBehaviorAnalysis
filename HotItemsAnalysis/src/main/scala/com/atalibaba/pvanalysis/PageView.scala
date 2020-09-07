package com.atalibaba.pvanalysis

import com.atalibaba.hotitemanalys.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author :YuFada
  * @date ： 2020/3/31 0031 上午 8:55
  *       Description：
  */

/*//输入数据样例类 上一个需求中有样例 这里不用重复写  有时可以写成bean封装起来
case class UserBehavior(userId: Long, iteamId: Long, categoryId: Int, behavior: String, timestamp: Long)*/
object PageView {
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
            .map(x => ("pv", 1)) // 来一个数据就转换为一个  ("pv",1)的二元粗  《类似于wordcount》
            .keyBy(_._1) // 利用pv（亚key）
            .timeWindow(Time.seconds(5))
            .sum(1)
            .print("PV:")

        env.execute("Page View Job")


    }

}
