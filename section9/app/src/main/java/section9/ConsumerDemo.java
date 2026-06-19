// lecture 50
package section9;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-group-id";

        log.info("Creating Kafka consumer properties");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");  // defines behavior when __consumer_offsets is empty or expired

        log.info("Creating Kafka consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            log.info("Polling...");
            ConsumerRecords<String, String> records = 
                consumer.poll(Duration.ofMillis(10000));
            
            for (ConsumerRecord<String, String> r : records) {
                log.info("\tKey: " + r.key() + ", Value: " + r.value());
                log.info("\tPartition: " + r.partition() + ", Offset: " + r.offset());
            }
        }
        // consumer.close();
        // log.info("Consumer closed");
    }
}
