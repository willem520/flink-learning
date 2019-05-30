package willem.weiyu.bigdata.flink.stream.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/27 14:33
 */
@Data
public class MyMetric {
   private String name;
   private long timestamp;
   private Map<String, Object> fields;
   private List<String> tags;
}
