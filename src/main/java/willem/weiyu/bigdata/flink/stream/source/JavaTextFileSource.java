package willem.weiyu.bigdata.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * @Author weiyu
 * @Description 基于文件的数据源
 * @Date 2019/2/27 12:24
 */
public class JavaTextFileSource {
    public static final String TEXT_PATH = "example" + File.separator + "word.txt";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.readTextFile(TEXT_PATH);
        lines.print();
        env.execute("javaTestFileSource");
    }
}
