package willem.weiyu.bigdata.flink.stream.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/27 14:43
 */
public class JavaKafkaSource {
    private static final String KAFKA_TOPIC = "flink-learn";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.26.27.81:9092");
        props.setProperty("group.id", "flink-test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer011 consumer = new FlinkKafkaConsumer011<>(KAFKA_TOPIC, new SimpleStringSchema(),props);
        //默认偏移量获取
//    consumer.setStartFromGroupOffsets()
        //从最早开始消费
        consumer.setStartFromEarliest();
        //指定消费位置
//    val specificStartOffsets = new util.HashMap[KafkaTopicPartition, Long]()
//    specificStartOffsets.put(new KafkaTopicPartition("fen_0310_test",0), 23L)
//    consumer.setStartFromSpecificOffsets(specificStartOffsets)
        DataStreamSource<String> kafkaStream = env.addSource(consumer);
        kafkaStream.print();
        env.execute("javaKafkaSource");
    }
}
