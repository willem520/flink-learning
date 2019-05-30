package willem.weiyu.bigdata.flink.cep;

import java.io.Serializable;

import lombok.Data;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/5/7 16:55
 */
@Data
public class LoginEvent implements Serializable {
    private String userId;
    private String ip;
    private String type;
    private long timestamp;
}
