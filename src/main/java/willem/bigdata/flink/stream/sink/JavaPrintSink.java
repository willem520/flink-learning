package willem.bigdata.flink.stream.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.io.File;

/**
 * @Author weiyu
 * @Description 打印的sink
 * @Date 2019/2/28 12:24
 */
public class JavaPrintSink {
    public static final String TEXT_PATH = "example" + File.separator + "word.txt";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.readTextFile(TEXT_PATH);
        // lines.print()内部调用的即为PrintSinkFunction
        lines.addSink(new PrintSinkFunction<>());
        env.execute("javaPrintSink");
    }
}
