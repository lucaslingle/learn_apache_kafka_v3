// lecture 49
package section9;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;  // <- not for prod
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Creating Kafka producer properties");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");  // <- not for prod
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); // <- not for prod

        log.info("Creating Kafka producer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello, Keyed World " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    
                Callback callback = new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info(
                                "Sent new record, received metadata:\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + key + "\n" +
                                "Partition: " + metadata.partition() + "\n"
                            );
                        } else {
                            log.error("Error while producing...\n", e);
                        }
                    }
                };
    
                producer.send(record, callback);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            producer.flush();
        }

        producer.close();
        log.info("Producer closed");
    }
}
