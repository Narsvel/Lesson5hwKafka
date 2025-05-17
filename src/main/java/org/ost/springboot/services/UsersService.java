package org.ost.springboot.services;

import org.ost.springboot.models.User;
import org.ost.springboot.repositories.UsersRepository;
import org.ost.springboot.utils.UserKafkaProducer;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@Transactional(readOnly = true) //все методы внутри класса будут по default Transactional только чтение
public class UsersService {

    private final UsersRepository usersRepository;
    private final UserKafkaProducer userKafkaProducer;

    public UsersService(UsersRepository usersRepository, UserKafkaProducer userKafkaProducer) {
        this.usersRepository = usersRepository;
        this.userKafkaProducer = userKafkaProducer;
    }

    public List<User> findAll() {
        return usersRepository.findAll();
    }

    public User findById(int id) {
        Optional<User> foundUser = usersRepository.findById(id);
        return foundUser.orElse(null);
    }

    @Transactional //т.к. метод не только читает данные, указываем аннотация Transactional, кторая заменит аннотицию класса
    public void save(User user) {
        userKafkaProducer.addNewUser(user.getEmail());
        usersRepository.save(user);
    }

    @Transactional
    public void update(int id, User updateUser) {
        Optional<User> foundUser = usersRepository.findById(id);
        foundUser.ifPresent(user -> updateUser.setCreateAt(user.getCreateAt()));
        updateUser.setId(id);
        usersRepository.save(updateUser);
    }

    @Transactional
    public void delete(int id) {
        Optional<User> user = usersRepository.findById(id);
        user.ifPresent(deleteUser -> userKafkaProducer.deleteUser(deleteUser.getEmail()));
        usersRepository.deleteById(id);
    }

}
