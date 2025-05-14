package org.ost.springboot.sandbox;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class KafkaConsumerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        //указываем порты для получения данных из kafka
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        //указываем класс который будет десериализировать ключ и данные
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //указываем смещение самое раннее сообщение в патрициях, чтобы получить сообщения отправленные ранее
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //подписка в рамках группы
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id"); //если не указать id то kafka назначит id сама и при перзапуске программы будет происходить перебалансировка группы т.к. id будет назначен kafka новый
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id"); //при иснсатнс id при перезапуске не будет происходить перебанассировки
        //отключаем автосмещение HEAD
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString()); //теперь HEAD нужно указывать при включении самостоятельно
        //условно говоря это сделано когда мы получили много данных и не смогли их все обработать (произошла ошибка), тогда нам необходимо вернуться к нужному коммиту, чтобы не потерять данные
        //для этого используется метод .commitSync(); или .commitAsync();

        //получаем по одной записи каждый раз
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");


        try (var consumer = new KafkaConsumer<String, String>(properties)) {

            consumer.subscribe(Pattern.compile("sandbox"), new MyConsumerRebalanceListener(consumer));


//            consumer.assign(List.of(            //указываем название топтка и партиции из которых мы хотим получать данные
//                    new TopicPartition("sandbox", 1)   //все подписки надо указать в рамках одного метода .assign
//            ));     //каждый последующий вызов .assign заменяет предыдущие данные подписки

//            consumer.seek(new TopicPartition("sandbox", 1),
//                    new OffsetAndMetadata(5));  //указываем смещение в топике sandbox в первой партиции на 5 позиций

//            consumer.seekToBeginning(List.of(new TopicPartition("sandbox", 1))); //получаем данные с начала партиции 1 топика sandbox
//            consumer.seekToEnd(List.of(new TopicPartition("sandbox", 1))); //получаем только те данные которые поступят в партицию 1 топика sandbox (HEAD в конце партиции)

            //предположим мы хотим увидеть данные начиная с определенной даты (условно программа не работала какое-то время и чтобы не пропустить данные)
//            Map<TopicPartition, OffsetAndTimestamp> offset = consumer. //метод передет смещение по времени(есть третий аргумент это время которое необходимо для нахождения нужного смещения в kafka, в больших проектах это может быть долгий процесс)
//                    offsetsForTimes(Map.of(new TopicPartition("sandbox", 1), 1746876699506L));
//            consumer.seek(new TopicPartition("sandbox", 1),
//                    offset.get(new TopicPartition("sandbox", 1)).offset()); //добавляем полученное смещение

            //для подписки в рамках группы используем
            //при этом данные будут получены только из одной париции, при повторном запросе из следующей партиции и так далее
//            consumer.subscribe(Pattern.compile("sandbox"), new MyConsumerRebalanceListener()); //если указат небольшое время для получения сообщений то kafka может не успеть перебалансировать группу и отправить сообщения

            //в ручную указываем топик и партиции
//            consumer.assign(List.of(            //указываем название топтка и партиции из которых мы хотим получать данные
//                    new TopicPartition("sandbox", 0),
//                    new TopicPartition("sandbox", 1),   //все подписки надо указать в рамках одного метода .assign
//                    new TopicPartition("sandbox", 2)
//            ));     //каждый последующий вызов .assign заменяет предыдущие данные подписки
//            consumer.unsubscribe(); //отписка от всех топиков

            //для постоянного опроса исползуем цикл и указываем задержку Duration.ofSeconds(1) 1 секунду
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(20)); //получаем данные из kafka в течении 20 секунд
//            StreamSupport.stream(records.spliterator(), false)  //проходим по List records и выводим данные в лог
//                    .forEach(record -> LOGGER.info("Record: {}", record));

//            consumer.commitSync(Map.of(new TopicPartition("sandbox", 1), new OffsetAndMetadata(5))); //указыаем смещение которое будет зафиксировано
            //или
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(20)); //получаем данные из kafka в течении 20 секунд
            StreamSupport.stream(records.spliterator(), false)  //проходим по List records и выводим данные в лог
                    .forEach(record -> {
                        LOGGER.info("Record: {}", record);
//                        consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
//                                new OffsetAndMetadata(record.offset() + 1))); //сохраняем смещение на следующем коммите
                        //или тот же результат
                        consumer.commitSync();
                        //можно использовать и
//                        consumer.commitAsync();
                    });



        }

    }

    static class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerApplication.class);

        private final KafkaConsumer<String, String> consumer;

        MyConsumerRebalanceListener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOGGER.info("Partitions revoked: {}", partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOGGER.info("Partitions assigned: {}", partitions);
            //после перебалансировки и назначению consumer группы в kafka будет задано смещение на указанную позицию если она есть в топике
//            if (partitions.contains(new TopicPartition("sandbox", 1))) {
//                consumer.seek(new TopicPartition("sandbox", 1), new OffsetAndMetadata(0));
//            }
        }
    }

}
