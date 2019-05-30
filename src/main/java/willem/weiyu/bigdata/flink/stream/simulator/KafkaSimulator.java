package willem.weiyu.bigdata.flink.stream.simulator;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import willem.weiyu.bigdata.flink.stream.model.MyMetric;

import java.util.*;

/**
 * @Author weiyu
 * @Description 模拟器
 * @Date 2019/2/27 14:58
 */
public class KafkaSimulator {
    public static final String BROKER_LIST = "10.26.27.81:9092";
    public static final String KAFKA_TOPIC = "flink-learn";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);
        while (true){
            MyMetric metric = buildMyMetric();
            String message = JSONObject.toJSONString(metric);
            ProducerRecord record = new ProducerRecord(KAFKA_TOPIC, JSONObject.toJSONString(message));
            producer.send(record);
            System.out.println("发送数据："+message);
            producer.flush();
            Thread.sleep(1000);
        }
    }

    private static MyMetric buildMyMetric(){
        MyMetric metric = new MyMetric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("flink");
        List<String> tags = new ArrayList<>();
        tags.add("fast");
        tags.add("scalable");
        metric.setTags(tags);
        Map<String, Object> fields = new HashMap<>();
        fields.put("t1","123");
        metric.setFields(fields);
        return metric;
    }
}
