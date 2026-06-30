// lecture 74-89
package section12;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonParser;

public class OpenSearchConsumer {
    private static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        URI connUri = URI.create(connString);
        String userInfo = connUri.getUserInfo();  // extract login information if it exists
        RestHighLevelClient restHighLevelClient;

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject()
            .get("meta").getAsJsonObject()
            .get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        String indexName = "wikimedia";

        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        String topicName = "wikimedia.recentchange";

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, exiting gracefully via consumer.wakeup().");
                consumer.wakeup(); // triggers an exception in consumer.
                try {
                    mainThread.join(); // wait for main to finish
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try (openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (!indexExists) {
                openSearchClient.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
                log.info("The '" + indexName + "' index has been created.");
            } else {
                log.info("The '" + indexName + "' index already exists.");
            }

            consumer.subscribe(Collections.singleton(topicName));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " records.");

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest(indexName)
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                        // IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        // log.info(indexResponse.getId());
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        // 
                    }
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records");

                    try {
                        Thread.sleep(Duration.ofMillis(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    consumer.commitSync();
                    log.info("Committed offsets.");
                }
            }
        }  catch (WakeupException e) {
            log.info("Consumer is starting to shut down.");
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        } finally {
            consumer.close(); // this will commit the offsets
            openSearchClient.close();
            log.info("Consumer gracefully shut down.");
        }
    }
}
