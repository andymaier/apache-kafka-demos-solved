package de.predic8.b_offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class OffsetConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(GROUP_ID_CONFIG, "offset");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe( singletonList("produktion"), new OffsetBeginningRebalanceListener(consumer, "produktion"));

            while(true) {
                ConsumerRecords<String, String> recs = consumer.poll(ofSeconds(1));
                if (recs.count() == 0)
                    continue;

                System.out.println(" Count: " + recs.count());

                for (ConsumerRecord<String, String> rec : recs)
                    System.out.printf("offset= %d, key= %s, value= %s\n", rec.offset(), rec.key(), rec.value());
            }
        }
    }

}
