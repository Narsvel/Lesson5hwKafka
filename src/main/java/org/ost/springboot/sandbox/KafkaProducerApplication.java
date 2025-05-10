package org.ost.springboot.sandbox;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaProducerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApplication.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        //указываем порты для передачи данных в kafka
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        //указываем класс который будет сериализировать ключ и данные
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //указываем транзакционный идентификатор отправителя (чтобы производить атомарные операции: несколько оперраций собраны в одну транзакцию)
//        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id"); //id должен быть уникален в рамках кластера

        //все транзакции в kafka по умолчанию иденпотентны, то есть они гарнтируют что сообщения будут добавлены в kafka или не добавлены вообще(? последнее не совсем так, все таки данные с отмененной транзакцией попадаю в kafka)
//        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.FALSE.toString()); //отключаем иденпотентность транзакций
        properties.put(ProducerConfig.ACKS_CONFIG, "1"); //иденпотентность транзакций если лидер записал транзакцию то все нормално
        //если указать 0 весто 1, то смещение в партиции в ответных metadata будет равно -1 т.к. мы не дожидаемся ответа от kafka с смещением которое было присвоено данной записи

        //KafkaProducer типизирован по ключу и значению
        try (var producer = new KafkaProducer<String, String>(properties)) {

//            producer.send(new ProducerRecord<>("sandbox", "Hello from Java!"),
//                    ((metadata, exception) -> LOGGER.info("Metadata: {}", metadata)));


            //если были добавлены настройки транзакции, то без использования транзакционных методов будет получена ошибка
//            producer.initTransactions();
//
//            producer.beginTransaction();
//            producer.send(new ProducerRecord<>("sandbox", "Hello from Java t1!"));
//            producer.send(new ProducerRecord<>("sandbox", "Hello from Java t2!"));
//            throw new IllegalArgumentException();
//
//            //если транзакцию не зафиксировать командой .commitTransaction() то она не будет зафиксирована kafka и откатится
//            producer.commitTransaction();

            //producer.abortTransaction(); отменяет транзакцию

//            RecordMetadata metadata = producer.send(new ProducerRecord<>("sandbox", "Hello from Java!"),
//                    (md, exception) -> {
//                LOGGER.info("Callback: metadata: {}, exception == null: {}", md, exception == null);
//                    }).get(); //если мы используем метод get то метод становится синхронным и дожидается ответа
//            //если метод get не вызывать то метод будет асинхронным и программа пойдет дальше не дожидаясь ответа от kafka
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");

//            RecordMetadata metadata = producer.send(new ProducerRecord<>("sandbox", "Hello from Java!")).get();
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");

//            metadata = producer.send(new ProducerRecord<>("sandbox","my-key", "Hello from Java with key!")).get();
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");
//
//            metadata = producer.send(new ProducerRecord<>("sandbox", 0,"my-key", "Hello from Java with key and partition!")).get();
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");
//
//            metadata = producer.send(new ProducerRecord<>("sandbox", 0,"my-key", "Hello from Java with key and partition!",
//                    List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");
//
//            metadata = producer.send(new ProducerRecord<>("sandbox", 1, System.currentTimeMillis(),"my-key-with-timestamp",
//                    "Hello from Java with key and partition and timestamp!")).get();
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");
//
//            metadata = producer.send(new ProducerRecord<>("sandbox", 1, System.currentTimeMillis(),"my-key-with-timestamp",
//                    "Hello from Java with key and partition and timestamp!",
//                    List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();
//            LOGGER.info("=============================================");
//            LOGGER.info("Metadata: {}", metadata);
//            LOGGER.info("=============================================");

        }

    }

}
