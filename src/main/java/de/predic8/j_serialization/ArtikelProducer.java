package de.predic8.j_serialization;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ArtikelProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ACKS_CONFIG, "all");
        props.put(RETRIES_CONFIG, 0);
        props.put(BATCH_SIZE_CONFIG, 16000);
        props.put(LINGER_MS_CONFIG, 100);
        props.put(BUFFER_MEMORY_CONFIG, 33554432);

        try(Producer<String, Artikel> producer = new KafkaProducer<String, Artikel>(props, new StringSerializer(), new ArtikelSerde())) {

            int i = 0;

            final var rand = new Random();
            for (; i < 10; i++) {
                final var artikel = new Artikel();
                artikel.setId(rand.nextInt(Integer.MAX_VALUE));
                artikel.setName("Artikelname " + rand.nextInt(10));
                artikel.setPrice(Math.abs(rand.nextFloat()));
                producer.send(new ProducerRecord<String, Artikel>("artikel", Long.toString(artikel.getId()), artikel));
            }
        }

    }
}
