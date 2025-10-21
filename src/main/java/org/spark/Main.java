package org.spark;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {


    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("Java Spark Application");

        //分區數
//        sparkConf.set("spark.default.parallelism","2");


        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 设定检查点路径:推荐HDFS共享文件系统，也可以使用本地文件路径
        javaSparkContext.setCheckpointDir("cp");

        List<Integer> list = Arrays.asList(1,5,32,7,11,1,2,2,3,4);

        //讀取文件
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("/Users/sai/IdeaProjects/Spark/inputData/data.txt");
        //保存文件
//        stringJavaRDD.saveAsTextFile("./output");
        //保存成對象(序列化後的)
//        stringJavaRDD.saveAsObjectFile("./output");
        //collect 行動算子(實際會讓data開始流動) *collect 結果存在內存生產環境慎用可能OOM 過大時需要保存在文件
//        stringJavaRDD.collect().forEach(System.out::println);

        // 讀取List參數: 來源數據 分區數
        //Spark在讀取集合數據時 分去設定優先級
        //1.方法參數
        //2.配置參數 spark.default.parallelism
        //3.採用環境變數
        JavaRDD<Integer> parallelize = javaSparkContext.parallelize(list,2);

        //** shuffle
        //把資料從多個分區 (partition) 重新分配到新的分區。
        //需要跨節點搬運資料，會觸發網路傳輸、磁碟 I/O(存磁碟會降低效率)。
        //是 Spark 最耗時、最昂貴 的操作之一。
//                | 方法                              | 主要用途  | 是否 shuffle | 適合場景                |
//                | -------------------------------- | -----    | ---------- | ------------------- |
//                | `coalesce(numPartitions, false)` | 減少分區  | 否          | 過濾後資料變少，快速合併分區      |
//                | `coalesce(numPartitions, true)`  | 減少分區  | 是          | 減少分區但要均勻分配          |
//                | `repartition(numPartitions)`     | 增/減分區 | 是          | 增加分區提高並行度，或減少分區但要均勻 |
        //縮減分區
        JavaRDD<Integer> coalesceRDD = parallelize.coalesce(2,true);
        //擴大分區
        JavaRDD<Integer> repartitionRDD = parallelize.repartition(2);


        //------ map:將傳入的Ａ轉成Ｂ 但沒有限制Ａ和Ｂ的關係(范行)
        JavaRDD<Object> mapRDD = parallelize.map(new Function<Integer, Object>() {
            @Override
            public Object call(Integer in) throws Exception {
                int out = in * 2;
                return out;
            }
        });

        //內存保存某RDD 原本是流數據在這不會存 有重複的操作一樣會全部重跑每個RDD 加上cache()保存後可以重這裡分岔計算
//        mapRDD.cache();
        //保存等級 保存在磁盤
//        mapRDD.persist(StorageLevel.MEMORY_AND_DISK());
        //checkpoint(): 保存至共用區域(HDFS共享文件系统)可以跨應用保存  检查点操作目的是希望RDD结果长时间的保存，所以需要保证数据的安全，会从头再跑一遍，性能比较低
        //为了提高效率，Spark推荐再检查点之前，执行cache方法，将数据缓存。
//        mapRDD.cache();
//        mapRDD.checkpoint();
        //釋放緩存
        mapRDD.unpersist();



        //簡化
//        JavaRDD<Object> mapRDD = parallelize.map(integer -> integer * 2);

        System.out.println("mapRDD:");
        mapRDD.collect().forEach(System.out::println);

        //------ filter:返回符合條件的 true false *會有數據傾斜的問題需要考慮
        JavaRDD<Integer> filterRDD = parallelize.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer in) throws Exception {
                return in % 2 == 0;
            }
        });
//        JavaRDD<Integer> filter = parallelize.filter(integer -> integer % 2 == 0);
        System.out.println("filter:");
        filterRDD.collect().forEach(System.out::println);

        //------ groupBy 分組 *可以重新訂分區 參數numPartitions 會打亂組合有可能會照成資源浪費 例如現在只有兩個分組 偶數 基數 如果預設3個分區會有一個浪費
        //groupBy 會有Shuffle操作會讀寫磁盤 (因為Spark是流只會短暫存在內存 如果需要等待計算需要完全寫入磁盤後才能讀流向下一個分區)
        //Shuffle效能低 需要確定邏輯與分區數量之間的分配
        //groupBy 返回後會變成KVJavaPairRDD(元組)
        JavaPairRDD<String, Iterable<Integer>> groupByRDD = parallelize.groupBy(new Function<Integer, String>() {
            @Override
            //根據返回規則分組
            public String call(Integer in) throws Exception {
                if (in % 2 == 0) {
                    return "偶數";//返回的key名
                } else {
                    return "基數";//返回的key名
                }
            }
            //參數numPartitions需要配合邏輯調整
        },2);

        System.out.println("groupByRDD:");
        groupByRDD.collect().forEach(System.out::println);
        //用groupByRDD再轉成物件
        List<GroupResult> resultList = groupByRDD.collect().stream()
                .map(tuple -> new GroupResult(tuple._1(), new ArrayList<>((Collection<Integer>)tuple._2())))
                .collect(Collectors.toList());
        resultList.forEach(System.out::println);


        //------ flatMap: 扁平化映射
        List<List<Integer>> lists = new ArrayList<>();
        lists.add(Arrays.asList(1,2));
        lists.add(Arrays.asList(3,4));
        JavaRDD<List<Integer>> parallelize2 = javaSparkContext.parallelize(lists, 2);
        JavaRDD<Integer> iteratorJavaRDD = parallelize2.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> ins) throws Exception {
                List<Integer> repLs = new ArrayList<>();
                ins.forEach(
                        n -> repLs.add(n * 2)
                );
                return repLs.iterator();
            }
        });
        System.out.println("iteratorJavaRDD:");
        iteratorJavaRDD.collect().forEach(System.out::println);

        //------ 去重 distinct 分布式去重
        JavaRDD<Integer> distinctRDD = parallelize.distinct();
        System.out.println("distinctRDD:");
        distinctRDD.collect().forEach(System.out::println);

        //------ 排序sortBy
        // 排序邏輯 這裡是給數據 標記
        // 升降序(ture升 false降)
        // 分區數
        JavaRDD<Integer> sortByRDD = parallelize.sortBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer in) throws Exception{
                // 使用int做標記會以數字排序
                return  in;
                //使用字串排序會依照字典
//              return ""+in;
            };
        },true,2);
        System.out.println("sortByRDD:");
        sortByRDD.collect().forEach(System.out::println);

        //------ 單值操作轉KK(Tuple) 元組操作
//        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = parallelize.mapToPair(in -> new Tuple2<>(in, in * 2));
        System.out.println("mapToPair:");
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = parallelize.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer in) throws Exception {
                return new Tuple2<String, Integer>(in.toString(), in * 2);
            }
        });
        stringIntegerJavaPairRDD.collect().forEach(System.out::println);


        // Tuple
//        Tuple（元組） 是一個不可變（immutable）的數據結構，用來存放固定數量、不同類型的元素。
//        和 List、Array 不同，Tuple 的長度是固定的，不能增減元素。
//        常用於 一次返回多個值 的場景。
//        Tuple2<String, Integer> tuple = new Tuple2<>("apple", 3);
//        // 取值
//        System.out.println("第一個元素: " + tuple._1()); // apple
//        System.out.println("第二個元素: " + tuple._2()); // 3

        //------ 元組(雙值操作 上面是單值) 使用parallelizePair處裡 處理 K V 不關聯的
        Tuple2<String, Integer> a = new Tuple2<>("a", 1);
        Tuple2<String, Integer> b = new Tuple2<>("b", 2);
        Tuple2<String, Integer> c = new Tuple2<>("c", 3);

        List<Tuple2<String, Integer>> tList = Arrays.asList(a, b, c);
        //使用 parallelizePairs
        JavaPairRDD<String, Integer> parallelizePairs = javaSparkContext.parallelizePairs(tList);
        JavaPairRDD<String, Object> pairRDD = parallelizePairs.mapValues(new Function<Integer, Object>() {
            @Override
            public Object call(Integer in) throws Exception {
                return in * 2;
            }
        });

        System.out.println("pairRDD:");
        pairRDD.collect().forEach(System.out::println);

        //------ groupByKeyRDD 根據Key進行分組(預聚和 效率高)
        Tuple2<String, Integer> a1 = new Tuple2<>("a", 1);
        Tuple2<String, Integer> b1 = new Tuple2<>("b", 2);
        Tuple2<String, Integer> a2 = new Tuple2<>("a", 3);
        Tuple2<String, Integer> b2 = new Tuple2<>("b", 4);
        List<Tuple2<String, Integer>> gList = Arrays.asList(a1, b1, a2, b2);

        JavaPairRDD<String, Integer> javaPairRDD = javaSparkContext.parallelizePairs(gList);
        System.out.println("groupByKeyRDD:");
        javaPairRDD.groupByKey().collect().forEach(System.out::println);

        //------ reduceByKeyRDD 在shuffle(落盤前會先預聚合效能高) 根據Key進行分組 再處理面的值(聚合成一個值)
        System.out.println("reduceByKey:");

//       使用自訂義分區器 javaPairRDD.reduceByKey(new MyPartitioner(2),new Function2<Integer, Integer, Integer>() {
        javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collect().forEach(System.out::println);

        //------ sortByKey 排序Key
        System.out.println("sortByKey:");
        javaPairRDD.sortByKey().collect().forEach(System.out::println);

        //釋放資源
        javaSparkContext.close();

        //分布式循環 速度慢沒有內存問題
        parallelize.foreach(System.out::println);
        //分布式循環 速度快但要考慮OO<
        parallelize.foreachPartition(System.out::println);

    }
    public static class GroupResult {
        private String key;
        private List<Integer> values;
        public GroupResult(String key, List<Integer> values) {
            this.key = key;
            this.values = values;
        }
        @Override
        public String toString() {
            return key + " => " + values;
        }
    }

    // 自定义分区器
//     1. 创建自定义类
//     2. 继承抽象类 Partitioner
//     3. 重写方法（2 + 2）
//         Partitioner(2) + Object(2)
//     4. 构建对象，在算子中使用
    static class MyPartitioner extends Partitioner {

        private int numPartitions;

        public MyPartitioner( int num ) {
            this.numPartitions = num;
        }


        @Override
        // 指定分区的数量
        public int numPartitions() {
            return this.numPartitions;
        }

        @Override
        // 根据数据的KEY来获取数据存储的分区编号，编号从0开始
        public int getPartition(Object key) {
            if ( "nba".equals(key) ) {
                return 0;
            } else if ( "wnba".equals(key) ) {
                return 1;
            } else {
                return 2;
            }
        }

        @Override
        public int hashCode() {
            return numPartitions;
        }

        @Override
        public boolean equals(Object o) {
            if ( o instanceof MyPartitioner ) {
                MyPartitioner other = (MyPartitioner)o;
                return this.numPartitions == other.numPartitions;
            } else {
                return false;
            }
        }
    }


}



