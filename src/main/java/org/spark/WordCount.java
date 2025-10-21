package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {


        // Word Count

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("Java Spark Application");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //讀取文件
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("/Users/sai/IdeaProjects/Spark/inputData/data.txt");
        //保存文件
//        stringJavaRDD.saveAsTextFile("./output");


        //扁平化處理 讀取是每行 要改成 讀取每個字
        JavaRDD<String> flatMapRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                //讀取是每行 要扁平化處理 讀取每個字
                List<String> list = Arrays.asList(s.split(" "));
                return list.iterator();
            }
        });
        System.out.println("-----------");
        flatMapRDD.collect().forEach(System.out::println);
        System.out.println("-----------");

        //分組
        JavaPairRDD<String, Iterable<String>> groupByRDD = flatMapRDD.groupBy(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s;
            }
        });
        System.out.println("-----------");
        groupByRDD.collect().forEach(System.out::println);
        System.out.println("-----------");

        //用mapValues 計算Value加上改變Value
        JavaPairRDD<String, Integer> mapValuesRDD = groupByRDD.mapValues(new Function<Iterable<String>, Integer>() {
            @Override
            public Integer call(Iterable<String> in) throws Exception {
                int i = 0;
                for (String s : in) {
                    i++;
                }
                return i;
            }
        });
        System.out.println("-----------");
        mapValuesRDD.collect().forEach(System.out::println);
        System.out.println("-----------");



        javaSparkContext.close();
    }


}
