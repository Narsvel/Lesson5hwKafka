package org.ost.springboot.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class UserKafkaProducer {

    //также можно добавить все параметры через application.properties, такой вариант в UserKafkaConsumer
    protected KafkaProducer<String, String> addKafkaProducer() {
        Properties proper = new Properties();
        //указываем порты для передачи данных в kafka
        proper.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        //указываем класс который будет сериализировать ключ и данные
        proper.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        proper.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(proper);
    }

    public void addNewUser(String email) {
        //использую 0 партицию в топике sandbox
        try (var producer = addKafkaProducer();) {
            producer.send(new ProducerRecord<>("sandbox", 0,"create", email));
        }
    }

    public void deleteUser(String email) {
        //использую 1 партицию в топике sandbox
        try (var producer = addKafkaProducer()) {
            producer.send(new ProducerRecord<>("sandbox", 1,"delete", email));
        }
    }

}
