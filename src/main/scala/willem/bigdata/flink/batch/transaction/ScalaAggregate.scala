package willem.bigdata.flink.batch.transaction

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._

/**
 * @author: willem 
 * @create: 2019/08/29 22:52
 * @description:
 */
object ScalaAggregate {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(
      (1, "Hello", 4),
      (1, "Hello", 5),
      (2, "Hello", 5),
      (3, "World", 6),
      (3, "World", 6)
    )
    val output = input.groupBy(1).aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 2)
    output.print()
    env.execute("scalaAggregate")
  }
}
