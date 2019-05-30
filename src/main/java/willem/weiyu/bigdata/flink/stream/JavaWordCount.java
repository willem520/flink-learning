package willem.weiyu.bigdata.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * @author weiyu
 * @description
 * @create 2018/5/2 10:25
 * @since 1.0.0
 */
public class JavaWordCount {
    private static final String TEXT_PATH = "example" + File.separator + "word.txt";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 算子设置并行度 > env 设置并行度 > 配置文件默认并行度 env.setParallelism(4);
        DataStreamSource<String> lines = env.readTextFile(TEXT_PATH);
        DataStream<WordWithCount> windowCounts = lines.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                for (String word : line.split("\\s+")) {
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .reduce((ReduceFunction<WordWithCount>) (a, b) -> new WordWithCount(a.word,a.count+b.count));

        windowCounts.print().setParallelism(1);
        env.execute("java wordCount demo");
    }

    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
