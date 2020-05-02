package com.jw.test01_hotItems;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            Thread.sleep(1000);
        }
    }

    private static void writeToKafka(String topic) throws IOException {
        Properties prop = new Properties();
        prop.put("bootstrap.server", "192.168.159.102:2181");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //从文件中读取数据
        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\JavaIDEAProject\\ProjectGroup\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            producer.send(record);
        }

    }

}
