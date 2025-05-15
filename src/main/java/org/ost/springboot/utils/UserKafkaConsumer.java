package org.ost.springboot.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class UserKafkaConsumer {

    private final EmailCreator emailCreator;

    public UserKafkaConsumer(EmailCreator emailCreator) {
        this.emailCreator = emailCreator;
    }

    @KafkaListener(
            topics = "${spring.kafka.topic.name}",
            topicPartitions = @TopicPartition(topic = "${spring.kafka.topic.name}", partitions = { "0", "1" })
    )
    public void consume(ConsumerRecord<String, String> record) {

        switch (record.key()) {
            case "create": emailCreator.messagesNewUser(record.value());
                break;
            case "delete": emailCreator.messagesDeleteUser(record.value());
        }

    }

}
