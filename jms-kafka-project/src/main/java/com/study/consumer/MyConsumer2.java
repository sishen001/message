package com.study.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer2 {

    public static Properties properties = new Properties();
    public static KafkaConsumer<String,String> consumer = null;
    static{
        properties.put("bootstrap.servers","192.168.1.12:9092,192.168.1.13:9092,192.168.1.14:9092");
        properties.put("group.id","testGroup");
        properties.put("enable.auto.commit","true");
        properties.put("auto.commit.interval.ms","1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(properties);
        //指定主题
        consumer.subscribe(Arrays.asList("test"));
        //指定某个主题某个分区
        //consumer.assign(Arrays.asList(new TopicPartition("test",0)));
    }

    //消费消息
    public static void consumerMsg(){
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(1000);
            for(ConsumerRecord<String,String> record : records){
                System.out.println("MyConsumer2offset= %d"+record.offset()+", k = ,"+record.key()+",value ="+record.value());
            }
            //if(records.count() > 0) consumer.commitSync();
        }
    }

    public static void main(String[] arg0) throws Exception{
        consumerMsg();
    }
}
