package org.sparkStreaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.ArrayList;
import java.util.HashMap;

//小批量處理數據流
public class sparkStreamingKafka {


    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkStreaming");
        final JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(3 * 1000));

        // 通过环境对象对接Socket数据源，获取数据模型，进行数据处理
        // 创建配置参数
        HashMap<String, Object> map = new HashMap<>();

        // 指定 Kafka 叢集的連線地址（可以是一個或多個 broker，用逗號分隔）
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

        // 指定 key 的反序列化器（消費時需要將 byte 轉回物件）
        // Kafka 裡的資料是二進位制傳輸的，這裡用 StringDeserializer 表示 key 是字串
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 指定 value 的反序列化器（同理，將 byte 轉為字串）
        // 若是傳 JSON、Avro 等格式，這裡要用對應的反序列化器
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 指定消費者所屬的組 ID（Consumer Group）
        // 同一組內的多個消費者會**共同分攤**一個 topic 的 partition 來消費
        // 若組名不同，則彼此獨立消費（各自會拿到完整的資料）
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"testGroup");

        // 指定當沒有初始 offset 或 offset 超出範圍時的策略：
        //   "earliest" -> 從最舊的資料開始讀（第一筆訊息）
        //   "latest"   -> 從最新的資料開始讀（新發送的訊息才會被消費）
        //   "none"     -> 如果沒有 offset 就報錯
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest"); // LEO

        // 需要消费的主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("test-topic");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jsc ,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(strings,map));

        directStream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> v1) throws Exception {
                return v1.value();
            }
        }).print(100);

        jsc.start();
        jsc.awaitTermination();

    }


}
