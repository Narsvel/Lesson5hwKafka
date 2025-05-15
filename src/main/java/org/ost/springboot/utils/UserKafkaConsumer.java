package org.ost.springboot.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class UserKafkaConsumer {

    @KafkaListener(
            topics = "${spring.kafka.topic.name}",
            topicPartitions = @TopicPartition(topic = "${spring.kafka.topic.name}", partitions = { "0", "1" })
    )
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("Record: Key - " + record.key() + ", Value - " + record.value());

    }

}
