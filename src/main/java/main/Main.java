package main.java.main;


import main.java.kafka.KafkaConsumerT;
import main.java.kafka.KafkaProducerT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        Thread kafkaProducerT = new KafkaProducerT();
        logger.info("kafkaProducerT start");
        kafkaProducerT.start();
        Thread kafkaConsumerT = new KafkaConsumerT();
        logger.info("kafkaConsumerT start");
        kafkaConsumerT.start();
    }
}
