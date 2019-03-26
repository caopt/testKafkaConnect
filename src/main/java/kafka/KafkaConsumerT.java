package main.java.kafka;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerT extends Thread{
    /**
     * logger
     */
    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerT.class);

    /**
     * Consumer statement
     */
    private Consumer<String, String> consumer;

    /**
     * KafkaConsumerT constructor
     */
    public KafkaConsumerT () {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.245.147:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group-1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer(props);
        this.consumer.subscribe(Collections.singletonList("cpt"));
    }

    @Override
    public void run() {
        ConsumerRecords<String, String> records = consumer.poll(100);
        consumer.commitAsync();
        for (ConsumerRecord<String, String> recode : records) {
            logger.info(recode.value());
            System.out.println(recode.value());
        }
    }
}
