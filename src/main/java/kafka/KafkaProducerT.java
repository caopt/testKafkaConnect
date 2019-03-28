package main.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerT extends Thread{
    /**
     * logger
     */
    private final Logger logger = LoggerFactory.getLogger(KafkaProducerT.class);
    private Producer<String, String> producer;
    /**
     * KafkaProducerT constructor
     */
    public KafkaProducerT() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.245.147:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer(props);
    }

    @Override
    public void run () {
        for (int i = 0; i < 100; i++) {
            try {
                sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
            if (i == 99) {
                producer.send(new ProducerRecord<String, String>("test", "exit", "exit"));
            }
        }
        logger.info("send over");
        producer.close();
    }
}
