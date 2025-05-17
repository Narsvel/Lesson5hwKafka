package org.ost.springboot.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.ost.springboot.repositories.UsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@EmbeddedKafka
@SpringBootTest
class UserKafkaConsumerTest {

    @MockitoBean
    private EmailCreator emailCreator;

    @Autowired
    UserKafkaConsumer userKafkaConsumer;

    @Test
    void consumeCreateRecordTest() {
        //Arrange
        String key = "create";
        String email = "test.mail.ru";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sandbox", 2, 0, key, email);
        //Act
        userKafkaConsumer.consume(record);
        //Assert
        ArgumentCaptor<String> emailCaptor = ArgumentCaptor.forClass(String.class);
        verify(emailCreator,times(1)).messagesNewUser(emailCaptor.capture());
        Assertions.assertEquals(email, emailCaptor.getValue());
    }

    @Test
    void consumeDeleteRecordTest() {
        //Arrange
        String key = "delete";
        String email = "test.mail.ru";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sandbox", 2, 0, key, email);
        //Act
        userKafkaConsumer.consume(record);
        //Assert
        ArgumentCaptor<String> emailCaptor = ArgumentCaptor.forClass(String.class);
        verify(emailCreator,times(1)).messagesDeleteUser(emailCaptor.capture());
        Assertions.assertEquals(email, emailCaptor.getValue());
    }
}