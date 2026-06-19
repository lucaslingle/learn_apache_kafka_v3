package section9;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Creating Kafka producer properties");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        log.info("Creating Kafka producer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>("kafka_basics_producer_demo", "Hello, World!"));
        producer.close();
        log.info("Producer closed");
    }
}
