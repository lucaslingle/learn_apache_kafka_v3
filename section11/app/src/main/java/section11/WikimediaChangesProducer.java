// lecture 61
package section11;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String topic = "wikimedia.recentchange";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler handler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url));
        EventSource source = builder.build();

        // starts another thread to listen
        source.start();

        // produce for 10 min and block til then
        TimeUnit.MINUTES.sleep(10);
    }
}
