package willem.bigdata.flink.cep;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/5/7 16:57
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginWarning implements Serializable {
    private String userId;
    private String ip;
    private String type;
    private long timestamp;
}
