package wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost",7777)
    // 处理，和批处理的操作一样
    val resultDataStream: DataStream[(String,Int)] =
      inputDataStream
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)//按照第一个分组
      .sum(1)//按照第二个元素求和
    resultDataStream.print()
    //执行
    env.execute("word count")
  }
}
