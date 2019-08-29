package willem.bigdata.flink.stream.source

import org.apache.flink.streaming.api.scala._
import willem.bigdata.flink.stream.StringLineEventSource

object ScalaCustomSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new StringLineEventSource)
    text.print

    env.execute("scala custom source")
  }
}
