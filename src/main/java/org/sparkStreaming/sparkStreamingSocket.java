package org.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//小批量處理數據流
public class sparkStreamingSocket {


    public static void main(String[] args) throws InterruptedException {

        // 構建環境對象
        //      Spark在流式數據的處理場景中對核心功能環境進行了封裝
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkStreaming");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));

        // 通過環境對象對接Socket數據源，獲取數據模型，進行數據處理 可以用 Netcat nc -lk 9999 發訊息測試
        final JavaReceiverInputDStream<String> socketDS = jsc.socketTextStream("127.0.0.1", 9999);

        //監聽並打印 類似 RDD的行動算子
//        socketDS.print();

        WordCount(socketDS);

        //啟動數據採集器
        jsc.start();


        // 窗口 : 其實就是數據的範圍（時間）
        //      window方法可以改變窗口的數據範圍（默認數據範圍為採集周期）JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
        //      window方法可以傳遞2個參數 **兩個參數都必須是 採集周期 的整數倍
        //             第一個 windowDuration 參數表示窗口的數據範圍（時間）
        //             第二個 slideDuration 參數表示窗口的移動幅度（時間），可以不用傳遞，默認使用的就是採集周期
        //      SparkStreaming是在窗口移動的時候計算的。

        // windowDuration slideDuration 一樣數據不會重複
        // windowDuration > slideDuration 數據會重複
        // windowDuration < slideDuration 數據會缺少
//        JavaDStream<String> window = socketDS.window(Durations.seconds(6),Durations.seconds(6));
//        window.print();


        //等待數據採集器的結束，如果採集器停止運行，那麼main線程會繼續執行
        jsc.awaitTermination();


        //  優雅關閉: close方法就是用於釋放資源，關閉環境，但是不能在當前main方法（線程）中完成
        new Thread(new Runnable() {
            @Override
            public void run() {
                while ( true ) {
                    //  關閉環境，釋放資源
                    try {
                        // boolean flg = false 關閉SparkStreaming的時候，需要在程序運行的過程中，通過外部操作進行關閉
                        Thread.sleep(5000);
                        //    MySQL JDBC : Table -> Col
                        //    Redis : Map -> KV
                        //    ZK : ZNode
                        //    HDFS : path
                        boolean flg = false;

                        if ( flg ) {
                            //jsc.close();//      強制地關閉
                            //jsc.stop();//      強制地關閉
                             jsc.stop(true, true);//      優雅地關閉(stop)
                        }
                    } catch ( Exception e ) {
                        //e.printStackTrace();
                    }
                }
            }
        }).start();

        // ** 數據採集器是一個長期執行的任務，所以不能停止，也不能釋放資源
//        jsc.close();

    }

    public static void WordCount(JavaReceiverInputDStream<String> socketDS){
        // Word Count
        // 扁平化處理 以空格分開
        JavaDStream<String> stringJavaDStream = socketDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                //讀取是每行 要扁平化處理 讀取每個字
                List<String> list = Arrays.asList(s.split(" "));
                return list.iterator();
            }
        });

        //改成Key Value
        final JavaPairDStream<String, Integer> wordDS = stringJavaDStream.mapToPair(
                word -> new Tuple2<>(word, 1)
        );


        // DStream確實就是對RDD的封裝，但是不是所有的方法都進行了分裝。有些方法不能使用：sortBy, sortByKey
        //      如果特定場合下，就需要使用這些方法，那麼就需要將DStream轉換為RDD使用

        final JavaPairDStream<String, Integer> wordCountDS = wordDS.reduceByKey(Integer::sum);

        wordCountDS.foreachRDD(
                rdd -> {
                    rdd.sortByKey().collect().forEach(System.out::println);
                }
        );


    }

}
