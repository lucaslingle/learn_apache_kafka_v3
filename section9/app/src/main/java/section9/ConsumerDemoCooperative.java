// lecture 54
package section9;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-group-id";

        log.info("Creating Kafka consumer properties");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        log.info("Creating Kafka consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

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

        try {
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
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down.");
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        } finally {
            consumer.close(); // this will commit the offsets
            log.info("Consumer gracefully shut down.");
        }
    }
}
