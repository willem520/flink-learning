package willem.bigdata.flink.batch.transaction

import org.apache.flink.api.scala._

/**
 * @author: willem 
 * @create: 2019/09/02 23:09
 * @description:
 */
object ScalaDistinct {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(
      (1, "Hello", 5.0),
      (1, "Hello", 4.0),
      (2, "Hello", 5.0),
      (3, "World", 7.0),
      (4, "World", 6.0)
    )
    input.distinct().print()
    println("======")
    input.distinct(0, 2)
    env.execute("scalaDistinct")
  }
}
