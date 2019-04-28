package com.study.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer {

    public static Properties properties = new Properties();
    public static Producer<String,String> producer = null;
    static{
        properties.put("bootstrap.servers","192.168.1.12:9092,192.168.1.13:9092,192.168.1.14:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }

    //同步发送
    public static void syncSend() throws Exception{

        for(int i = 0; i < 447; i++) {
            //同步方式发送消息
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-pengfei", Integer.toString(i));
            Future<RecordMetadata> result = producer.send(producerRecord);

            //等待消息发送成功的同步阻塞方法
            RecordMetadata metadata = result.get();
            System.out.println("同步方式发送消息结果：" + "topic=" + metadata.topic() + "|partition=" + metadata.partition() + "|offset=" + metadata.offset());
        }
    }

    //异步发送
    public static void asyncSend(){

        for(int i = 0; i < 15; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test3", Integer.toString(i));
            //异步发送方式
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(null != e){
                        System.out.println("消息发送失败");
                    }

                    if(null != metadata){
                        System.out.println("异步发送消息结果："+"topic="+metadata.topic() + "|partition="+metadata.partition()+"|offset="+metadata.offset());
                    }
                }
            });
        }
    }
    public static void main(String[] arg0) throws Exception{
        syncSend();
        //asyncSend();
    }
}
