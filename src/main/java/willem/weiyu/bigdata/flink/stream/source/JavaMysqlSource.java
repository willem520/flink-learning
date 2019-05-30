package willem.weiyu.bigdata.flink.stream.source;

import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author weiyu
 * @Description 自定义mysqlSource
 * @Date 2019/2/27 16:33
 */
public class JavaMysqlSource {
    private static final Logger logger = LoggerFactory.getLogger(JavaMysqlSource.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<EpConfigSource> dataStream = env.addSource(new MysqlSource());
        dataStream.print();
        env.execute("javaMysqlSource");
    }
}

class MysqlSource extends RichSourceFunction <EpConfigSource> {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void run(SourceContext<EpConfigSource> sourceContext) throws Exception {
        try {
            // 4.执行查询，封装数据
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet == null){
                return;
            }
            while (resultSet.next()) {
                EpConfigSource bean = new EpConfigSource();
                bean.setId(resultSet.getLong("id"));
                bean.setName(resultSet.getString("name"));
                bean.setHost(resultSet.getString("host"));
                bean.setPort(resultSet.getInt("port"));
                bean.setUsername(resultSet.getString("username"));
                bean.setPassword(resultSet.getString("password"));
                bean.setStatus(resultSet.getInt("status"));
                bean.setDbName(resultSet.getString("db_name"));
                bean.setEpHost(resultSet.getString("ep_host"));
                bean.setCtime(resultSet.getTimestamp("ctime").getTime());
                sourceContext.collect(bean);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://10.26.15.199:3306/das?characterEncoding=utf8&useSSL=true";
        String username = "root";
        String password = "3ae560c8e28f6faf";
        // 1.加载驱动
        Class.forName(driver);
        // 2.创建连接
        connection = DriverManager.getConnection(url, username, password);
        // 3.获得执行语句
        String sql = "SELECT * FROM das_ep_config_source";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
}

@Data
class EpConfigSource{
    private long id;
    private String name;
    private String host;
    private int port;
    private String username;
    private String password;
    private int status;
    private String dbName;
    private String epHost;
    private long ctime;
}
