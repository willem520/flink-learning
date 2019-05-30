package willem.weiyu.bigdata.flink.stream.sink;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/5/9 17:49
 */
public class JavaEsSink {
    public static final String TEXT_PATH = "example" + File.separator + "word.txt";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> lines = env.readTextFile(TEXT_PATH);

        // lines.print()内部调用的即为PrintSinkFunction
        List<HttpHost> esHosts = new ArrayList<>();
        esHosts.add(new HttpHost("127.0.0.1",9200,"http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(esHosts,
                new ElasticsearchSinkFunction<String>() {
                    private IndexRequest createIndexRequest(String s) {
                        Map<String,String> data = new HashMap<>();
                        data.put("data",s);
                        return Requests.indexRequest()
                                .index("flink_es_sink")
                                .type("flink_es_sink")
                                .source(data);
                    }
                    @Override
                    public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(s));
                    }
                });
        esSinkBuilder.setBulkFlushMaxActions(1);

        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setMaxRetryTimeoutMillis(3000);
                    Header[] headers = new BasicHeader[]{new BasicHeader("Content-Type","application/json")};
                    restClientBuilder.setDefaultHeaders(headers);
                    //restClientBuilder.setPathPrefix("");
                    restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                AuthScope.ANY, new UsernamePasswordCredentials("user", "password"));
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    });
                }
        );

        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        lines.addSink(esSinkBuilder.build());
        env.execute("javaPrintSink");
    }
}
