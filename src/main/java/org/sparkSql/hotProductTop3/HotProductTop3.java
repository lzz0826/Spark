package org.sparkSql.hotProductTop3;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.udaf;

//  需求：
//    Spark Core :      熱門品類Top10
//    Spark SQL  : 各區域熱門商品Top3

//  1. 如果需求中有【各個XXXX】描述
//      表述的基本含義就是相同的數據分在一個組中 ：group by
//  2. 熱門：只從點擊量統計 => (商品ID, 點擊數量)
//  3. Top3: （組內）排序后取前3名
        /*
        區域      商品      點擊數量    排序號
        -----------------------------------
        華北      鞋       5000        1
        華北      鞋       5000        1
        華北      鞋       5000        1
        華北      鞋       5000        1
        華北      鞋       5000        1
        華北      衣服     3500        2
        華北      帽子     1500        3
        東北      鞋       6000        1
        東北      手機     5500        2
        東北      電腦     5200        3

        ---------------------------------------------------------------------
        區域      商品      城市      點擊數量     總的點擊數量     比率      順寫號
        ---------------------------------------------------------------------
        華北      鞋       北京      5000        14000         5/14      1
        華北      鞋       天津      4000        14000         4/14      2
        華北      鞋       保定      3000        14000         3/14      3
        華北      鞋       石家莊    2000        14000         2/14      4


        最終希望:
        ---------------------------------------------------------------------
        區域      商品      城市      點擊數量     城市備注
        ---------------------------------------------------------------------
        華北      鞋       北京      5000        北京21.2%   天津13.2%   其他65.6%
        華北      鞋       天津      4000        北京63.0%   天津10%     其他27%

       多什麼，刪什麼 缺什麼，補什麼
       原本就有:區域.商品.城市
       需補上:點擊數量.城市備注

        1. 行轉列？
        2. 如何拼接結果

        SQL不能做或不好做的功能，可以採用自定義 UDF，UDAF 函數來實現

        需求的實現方式：SQL + UDAF
         */

public class HotProductTop3 {

    public static void main(String[] args) {

        SparkSession hadoopConnect = SparkUtil.getHadoopConnect();

        //註冊自定義 函數
        hadoopConnect.udf().register("myUDAF", udaf(new MyUDAF(), Encoders.STRING()));

        //初始化數據
//        initHadoopData(hadoopConnect);

        doSomething(hadoopConnect);

        //釋放資源
        hadoopConnect.close();

    }

    //初始化數據
    public static void initHadoopData(SparkSession sparkSession){

        sparkSession.sql("show tables").show();

        //資料結構:
        //2019-07-17	95	26070e87-1ad7-49a3-8fb3-cc741facaddf	37	2019-07-17 00:00:02	手机	-1	-1	\N	\N	\N	\N	3
        //對應下面表的順序
        sparkSession.sql("CREATE TABLE `user_visit_action`(\n" +
                "  `date` string,\n" +
                "  `user_id` bigint,\n" +
                "  `session_id` string,\n" +
                "  `page_id` bigint,\n" +
                "  `action_time` string,\n" +
                "  `search_keyword` string,\n" +
                "  `click_category_id` bigint,\n" +
                "  `click_product_id` bigint, --點擊商品id，沒有商品用-1表示。\n" +
                "  `order_category_ids` string,\n" +
                "  `order_product_ids` string,\n" +
                "  `pay_category_ids` string,\n" +
                "  `pay_product_ids` string,\n" +
                "  `city_id` bigint --城市id\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        sparkSession.sql("load data local inpath '/Users/sai/IdeaProjects/Spark/inputData/user_visit_action2.txt' into table user_visit_action;");

        //資料結構: 1	北京	华北
        sparkSession.sql("CREATE TABLE `city_info`(\n" +
                "  `city_id` bigint, --城市id\n" +
                "  `city_name` string, --城市名稱\n" +
                "  `area` string --區域名稱\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");


        //資料結構: 1	商品_1	自营
        sparkSession.sql("CREATE TABLE `product_info`(\n" +
                "  `product_id` bigint, -- 商品id\n" +
                "  `product_name` string, --商品名稱\n" +
                "  `extend_info` string\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        sparkSession.sql("load data local inpath 'inputData/city_info.txt' into table city_info;");
        sparkSession.sql("load data local inpath 'inputData/product_info.txt' into table product_info;");

        sparkSession.sql("select * from city_info limit 10").show();

    }

    //這裡用到的表 對應 initHadoopData 裡放的表
    public static void doSomething(SparkSession sparkSession){


        // STEP 1：建立 t1 表（明細資料）
        sparkSession.sql("SELECT ci.area , uva.click_product_id , pi.product_name , ci.city_name\n" +
                "FROM user_visit_action uva\n" +
                "JOIN product_info pi\n" +
                "ON uva.click_product_id = pi.product_id\n" +
                "JOIN city_info ci\n" +
                "ON uva.city_id = ci.city_id\n" +
                "WHERE uva.click_product_id != -1").createOrReplaceTempView("t1"); //將數據模型轉換成表，方便SQL的使用

        System.out.println("-------- t1 表結果   --------");

        sparkSession.sql("show tables").show();
        sparkSession.sql("SELECT * FROM t1").show();

        /*
        +----+----------------+------------+---------+
        |area|click_product_id|product_name|city_name|
        +----+----------------+------------+---------+
        |华北|              98|     商品_98|     保定|
        |华北|              85|     商品_85|     天津|
        |华中|              36|     商品_36|     武汉|
        |华南|              44|     商品_44|     广州|
        |华东|              79|     商品_79|     上海|
        |华南|              50|     商品_50|     厦门|
        |东北|              39|     商品_39|     大连|
        |华南|              62|     商品_62|     福州|
        |东北|              58|     商品_58|   哈尔滨|
        |华东|              68|     商品_68|     无锡|
        |西北|              45|     商品_45|     银川|
        |华东|              92|     商品_92|     济南|
        |华南|              93|     商品_93|     深圳|
        |华东|              11|     商品_11|     南京|
        |西北|               3|      商品_3|     西安|
        |东北|              30|     商品_30|     沈阳|
        |西北|              54|     商品_54|     银川|
        |西南|              71|     商品_71|     成都|
        |东北|              79|     商品_79|   哈尔滨|
        |西北|              39|     商品_39|     银川|
        +----+----------------+------------+---------+
        */

        System.out.println("-------- t1 表結果   --------");


        // STEP 2：建立 t2 表（每地區 + 商品的點擊統計與城市備註）
        sparkSession.sql("SELECT\n" +
                "            area,\n" +
                "            click_product_id,\n" +
                "            product_name,\n" +
                "            COUNT(*) AS clickCnt,\n" +
                "            myUDAF(city_name) AS cityremark\n" +
                "        FROM t1\n" +
                "        GROUP BY area, click_product_id, product_name").createOrReplaceTempView("t2");


        System.out.println("-------- t2 表結果   --------");

        sparkSession.sql("SELECT * FROM t2").show();
        /*
        +----+----------------+------------+--------+--------------------------+
        |area|click_product_id|product_name|clickCnt|                cityremark|
        +----+----------------+------------+--------+--------------------------+
        |东北|               1|      商品_1|     130|哈尔滨 29%沈阳 32%其他 39%|
        |东北|               2|      商品_2|     152|哈尔滨 29%大连 32%其他 39%|
        |东北|               3|      商品_3|     145|大连 29%哈尔滨 31%其他 40%|
        |东北|               4|      商品_4|     141|沈阳 30%哈尔滨 34%其他 36%|
        |东北|               5|      商品_5|     151|  大连 27%沈阳 35%其他 38%|
        |东北|               6|      商品_6|     135|哈尔滨 31%大连 33%其他 36%|
        |东北|               7|      商品_7|     143|大连 31%哈尔滨 33%其他 36%|
        |东北|               8|      商品_8|     129|哈尔滨 29%沈阳 31%其他 40%|
        |东北|               9|      商品_9|     139|大连 32%哈尔滨 33%其他 35%|
        |东北|              10|     商品_10|     146|沈阳 28%哈尔滨 34%其他 38%|
        |东北|              11|     商品_11|     137|  大连 24%沈阳 35%其他 41%|
        |东北|              12|     商品_12|     152|沈阳 30%哈尔滨 33%其他 37%|
        |东北|              13|     商品_13|     158|大连 29%哈尔滨 32%其他 39%|
        |东北|              14|     商品_14|     143|  大连 30%沈阳 34%其他 36%|
        |东北|              15|     商品_15|     147|大连 29%哈尔滨 33%其他 38%|
        |东北|              16|     商品_16|     139|哈尔滨 32%沈阳 33%其他 35%|
        |东北|              17|     商品_17|     136|大连 29%哈尔滨 32%其他 39%|
        |东北|              18|     商品_18|     130|  沈阳 31%大连 32%其他 37%|
        |东北|              19|     商品_19|     130|大连 26%哈尔滨 34%其他 40%|
        |东北|              20|     商品_20|     122|  沈阳 28%大连 30%其他 42%|
        +----+----------------+------------+--------+--------------------------+
        */
        System.out.println("-------- t2 表結果   --------");

        // STEP 3：建立 t3 表（加上排名）
        sparkSession.sql("SELECT\n" +
                "            *,\n" +
                "            RANK() OVER (PARTITION BY area ORDER BY clickCnt DESC) AS rk\n" +
                "        FROM t2").createOrReplaceTempView("t3");

        System.out.println("-------- t3 表結果 --------");
        sparkSession.sql("SELECT * FROM t3 ").show();

        /*
        +----+----------------+------------+--------+--------------------------+---+
        |东北|              41|     商品_41|     169|  沈阳 29%大连 34%其他 37%|  1|
        |东北|              91|     商品_91|     165|  沈阳 31%大连 32%其他 37%|  2|
        |东北|              58|     商品_58|     159|哈尔滨 30%大连 32%其他 38%|  3|
        |东北|              93|     商品_93|     159|  沈阳 24%大连 37%其他 39%|  3|
        |东北|              13|     商品_13|     158|大连 29%哈尔滨 32%其他 39%|  5|
        |东北|              83|     商品_83|     158|哈尔滨 31%沈阳 32%其他 37%|  5|
        |东北|              37|     商品_37|     155|哈尔滨 25%沈阳 34%其他 41%|  7|
        |东北|              27|     商品_27|     154|大连 31%哈尔滨 33%其他 36%|  8|
        |东北|              32|     商品_32|     154|哈尔滨 31%沈阳 33%其他 36%|  8|
        |东北|              72|     商品_72|     154|哈尔滨 29%沈阳 31%其他 40%|  8|
        |东北|               2|      商品_2|     152|哈尔滨 29%大连 32%其他 39%| 11|
        |东北|              12|     商品_12|     152|沈阳 30%哈尔滨 33%其他 37%| 11|
        |东北|              86|     商品_86|     152|  沈阳 26%大连 34%其他 40%| 11|
        |东北|               5|      商品_5|     151|  大连 27%沈阳 35%其他 38%| 14|
        |东北|              25|     商品_25|     151|哈尔滨 29%沈阳 34%其他 37%| 14|
        |东北|              39|     商品_39|     148|沈阳 25%哈尔滨 29%其他 46%| 16|
        |东北|              55|     商品_55|     148|大连 27%哈尔滨 34%其他 39%| 16|
        |东北|              60|     商品_60|     148|  沈阳 31%大连 33%其他 36%| 16|
        |东北|              66|     商品_66|     148|哈尔滨 32%大连 32%其他 36%| 16|
        |东北|              75|     商品_75|     148|大连 27%哈尔滨 36%其他 37%| 16|
        +----+----------------+------------+--------+--------------------------+---+
        */

        System.out.println("-------- t3 表結果 --------");

        // STEP 4：輸出每個地區 Top 3 商品
        sparkSession.sql("SELECT\n" +
                "            area,\n" +
                "            product_name,\n" +
                "            clickCnt,\n" +
                "            cityremark,\n" +
                "            rk\n" +
                "        FROM t3\n" +
                "        WHERE rk <= 3\n" +
                "        ORDER BY area, rk").show();
        System.out.println("-------- 各地區 Top3 商品 --------");
        /*
        +----+------------+--------+--------------------------+---+
        |area|product_name|clickCnt|                cityremark| rk|
        +----+------------+--------+--------------------------+---+
        |东北|     商品_41|     169|  沈阳 29%大连 34%其他 37%|  1|
        |东北|     商品_91|     165|  沈阳 31%大连 32%其他 37%|  2|
        |东北|     商品_58|     159|哈尔滨 30%大连 32%其他 38%|  3|
        |东北|     商品_93|     159|  沈阳 24%大连 37%其他 39%|  3|
        |华东|     商品_86|     371|  苏州 11%青岛 11%其他 78%|  1|
        |华东|     商品_75|     366|  南京 10%杭州 12%其他 78%|  2|
        |华东|     商品_47|     366|  苏州 12%无锡 13%其他 75%|  2|
        |华中|     商品_62|     117|          长沙 48%武汉 51%|  1|
        |华中|      商品_4|     113|          武汉 46%长沙 53%|  2|
        |华中|     商品_29|     111|          长沙 49%武汉 50%|  3|
        |华中|     商品_57|     111|          长沙 45%武汉 54%|  3|
        |华北|     商品_42|     264|石家庄 15%北京 17%其他 68%|  1|
        |华北|     商品_99|     264|保定 15%石家庄 17%其他 68%|  1|
        |华北|     商品_19|     260|天津 18%石家庄 18%其他 64%|  3|
        |华南|     商品_23|     224|  广州 21%福州 24%其他 55%|  1|
        |华南|     商品_65|     222|  广州 22%福州 22%其他 56%|  2|
        |华南|     商品_50|     212|  厦门 22%广州 24%其他 54%|  3|
        |西北|     商品_15|     116|          银川 45%西安 54%|  1|
        |西北|      商品_2|     114|          西安 46%银川 53%|  2|
        |西北|     商品_22|     113|          银川 45%西安 54%|  3|
        +----+------------+--------+--------------------------+---+
        */
    }


}



