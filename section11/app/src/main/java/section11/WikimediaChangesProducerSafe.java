// lecture 68
package section11;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;

public class WikimediaChangesProducerSafe {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // using the defaults in kafka 3.0+ but setting them here explicitly below.
        // somehow should set broker-side min.insync.replicas = 2, 
        // but not here since this is producer only.
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // > 5 breaks idempotence support
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

        String topic = "wikimedia.recentchange";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler handler = new WikimediaChangeHandler(producer, topic);
        Headers headers = new Headers.Builder()
                .add("User-Agent", "WikimediaChangesProducer/1.0 (kafka-learning)")
                .build();
        EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url))
                .headers(headers);
        EventSource source = builder.build();

        // starts another thread to listen
        source.start();

        // produce for 10 min and block til then
        TimeUnit.MINUTES.sleep(10);
    }
}
