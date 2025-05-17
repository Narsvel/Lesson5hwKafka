package org.ost.springboot.utils;

import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.ost.springboot.models.User;
import org.ost.springboot.repositories.UsersRepository;
import org.ost.springboot.services.UsersService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Optional;

import static org.mockito.Mockito.*;

@SpringBootTest
class UserKafkaProducerTest {

    @MockitoBean
    private UsersRepository usersRepository;
    @MockitoBean
    private UserKafkaProducer userKafkaProducer;

    @Autowired
    private UsersService usersService;

    @Test
    void addNewUserTest() throws InterruptedException {
        //Arrange
        String email = "test.mail.ru";
        User user = new User();
        user.setEmail(email);
        //Act
        usersService.save(user);
        //Assert
        ArgumentCaptor<String> emailCaptor = ArgumentCaptor.forClass(String.class);
        verify(userKafkaProducer,times(1)).addNewUser(emailCaptor.capture());
        Assertions.assertEquals(email, emailCaptor.getValue());
    }

    @Test
    void deleteUserTest() {
        //Arrange
        int id = 1;
        String email = "test.mail.ru";
        User user = new User();
        user.setEmail(email);
        when(usersRepository.findById(id)).thenReturn(Optional.of(user));
        //Act
        usersService.delete(id);
        //Assert
        ArgumentCaptor<String> emailCaptor = ArgumentCaptor.forClass(String.class);
        verify(userKafkaProducer,times(1)).deleteUser(emailCaptor.capture());
        Assertions.assertEquals(email, emailCaptor.getValue());
    }
}