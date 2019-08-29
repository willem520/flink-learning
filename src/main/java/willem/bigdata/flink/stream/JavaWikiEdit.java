package willem.bigdata.flink.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import scala.Tuple2;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/5/10 16:19
 */
public class JavaWikiEdit {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.addSource(new WikipediaEditsSource())
                .keyBy((KeySelector<WikipediaEditEvent, String>) wikiEditEvent-> wikiEditEvent.getUser())
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return new Tuple2("",0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(WikipediaEditEvent wikipediaEditEvent,
                                                       Tuple2<String, Integer> stringIntegerTuple2) {
                        return new Tuple2(wikipediaEditEvent.getUser(),
                                wikipediaEditEvent.getByteDiff()+stringIntegerTuple2._1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> stringIntegerTuple2) {
                        return stringIntegerTuple2;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2,
                                                         Tuple2<String, Integer> acc1) {
                        return new Tuple2(stringIntegerTuple2._1, stringIntegerTuple2._1+acc1._1);
                    }
                }).map(i->i.toString()).print();

        streamEnv.execute("javaWikiEdit");
    }
}

