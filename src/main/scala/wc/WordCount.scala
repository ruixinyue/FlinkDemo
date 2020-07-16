package wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("D:\\Program Files\\JetBrains\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\word.txt")
    // 切分，转换成元组，group by，sum
    val resultDataSet: DataSet[(String,Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    resultDataSet.print()
  }
}
