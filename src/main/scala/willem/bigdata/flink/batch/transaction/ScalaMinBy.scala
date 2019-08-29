package willem.bigdata.flink.batch.transaction

import org.apache.flink.api.scala._

/**
 * @author: willem 
 * @create: 2019/08/29 23:12
 * @description:
 */
object ScalaMinBy {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(
      (1, "Hello", 5.0),
      (1, "Hello", 4.0),
      (2, "Hello", 5.0),
      (3, "World", 7.0),
      (4, "World", 6.0)
    )
    //根据第二个元素做分组后，现对第一个元素求最小值，如果相同，再对第三个元素求最小值，maxBy与之类似
    val output = input.groupBy(1).minBy(0,2)
    output.print()
    env.execute("scalaMinBy")
  }
}
