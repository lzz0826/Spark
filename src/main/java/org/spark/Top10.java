package org.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class Top10 {

// IdeaProjects/Spark/inputData/user_visit_action.txt

//編號          	                        字段類型	        字段含義
//1 	date	                        String	        用戶點擊行為的日期
//2	    user_id	                        Long	        用戶的ID
//3	    session_id	                    String	        Session的ID
//4	    page_id     	                Long	        某個頁面的ID
//5	    action_time 	                String	        動作的時間點(時間戳)
//6	    search_keyword	                String	        用戶搜索的關鍵詞
//7	    click_category_id(品類ID)	    Long	        某一個商品品類的ID
//8	    click_product_id	            Long	        某一個商品的ID
//9	    order_category_ids(下單品類ID)	String	        一次訂單中所有品類的ID集合
//10	order_product_ids	            String	        一次訂單中所有商品的ID集合
//11	pay_category_ids(支付品類ID)	    String	        一次支付中所有品類的ID集合
//12	pay_product_ids	                String	        一次支付中所有商品的ID集合
//13	city_id	Long	                城市             城市id

//    需求說明
//    品類是指產品的分類，大型電商網站品類分多級，咱們的項目中品類只有一級，不同的公司可能對熱門的定義不一樣，我們按照每個品類的點擊，下單，支付的量來統計熱門品類：
//    鞋 點擊數 下單數 支付數
//    衣服 點擊數 下單數 支付數
//    電腦 點擊數 下單數 支付數
//
//    例如：綜合排名 = 點擊數 * 20% + 下單數 * 30% + 支付數 * 50%
//    本項目需求優化為：先按照點擊數排名，靠前的就排名高；如果點擊數相同，再比較下單數；下單數再相同，就比較支付數。

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("Java Spark Application");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //讀取文件
        //2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_29_2019-07-17 00:00:19_null_12_36_null_null_null_null_5
        //日期_用戶ID_SessionID_頁面ID_動作時間_收索關鍵字_一次訂單中所有品類的ID集合_一次訂單中所有商品的ID集合_一次支付中所有品類的ID集合_一次支付中所有商品的ID集合_城市id
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("./inputData/user_visit_action.txt");

        // 開發原則
        //      大數據開發：數據量大
        //      1. 多什麼，刪什麼（減小數據規模）
        //          去除搜索數據
        //          對於的數據字段
        //      2. 缺什麼，補什麼
        //          品類。行為類型
        //      3. 功能實現中，要盡可能少地使用shuffle操作。shuffle越多，性能越低。

        // 需求分析
        //      熱門( 點擊數量，下單數量，支付數量 )品類Top10
        //      1. 對同一個品類的不同行為進行統計
        //         對同一個品類的數據進行分組（點擊，下單，支付）。
        //         (品類id，點擊數量)
        //         (品類id，下單數量)
        //         (品類id，支付數量)
        //      2. 對統計結果進行排序
        //         (user, age, amount) 先按照年齡排序，再按照金額排序
        //         (品類id, 點擊數量，下單數量，支付數量 )
        //      3. 對排序后的結果取前10條
        //         first(), take(10)


        //轉成物件處理
        JavaRDD<CategoryCountInfo> categoryCountInfoRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, CategoryCountInfo>() {
            @Override
            public Iterator<CategoryCountInfo> call(String v1) throws Exception {
                String[] split = v1.split("_");
                List<CategoryCountInfo> categoryCountInfo = new ArrayList<>();
                // 點擊數
                if(!split[6].equals("-1")){
                    categoryCountInfo.add(new CategoryCountInfo(split[6], 1L, 0L, 0L));
                }
                // 下單數
                if(!split[8].equals("null")){
                    String[] orderCountSplit = split[8].split(",");
                    for (String s : orderCountSplit) {
                        categoryCountInfo.add(new CategoryCountInfo(s, 0L, 1L, 0L));
                    }
                }
                // 支付數
                if(!split[10].equals("null")){
                    String[] payCountSplit = split[10].split(",");
                    for (String s : payCountSplit) {
                        categoryCountInfo.add(new CategoryCountInfo(s, 0L, 0L, 1L));
                    }
                }
                return categoryCountInfo.iterator();
            }
        });

        //用 CategoryId 當key之後用reduceByKey來處理
        JavaPairRDD<String, CategoryCountInfo> categoryCountInfoJavaPairRDD = categoryCountInfoRDD.mapToPair(
                new PairFunction<CategoryCountInfo, String, CategoryCountInfo>() {
                    @Override
                    public Tuple2<String, CategoryCountInfo> call(CategoryCountInfo info) throws Exception {
                        return new Tuple2<>(info.getCategoryId(), info);
                    }
                }
        ).reduceByKey(new Function2<CategoryCountInfo, CategoryCountInfo, CategoryCountInfo>() {
            @Override
            public CategoryCountInfo call(CategoryCountInfo info1, CategoryCountInfo info2) throws Exception {
                CategoryCountInfo categoryCountInfo = new CategoryCountInfo();
                categoryCountInfo.setCategoryId(info1.getCategoryId());
                categoryCountInfo.setClickCount(info1.getClickCount() + info2.getClickCount());
                categoryCountInfo.setOrderCount(info1.getOrderCount() + info2.getOrderCount());
                categoryCountInfo.setPayCount(info1.getPayCount() + info2.getPayCount());
                return categoryCountInfo;
            }
        });

//        先按照點擊數排名，靠前的就排名高；如果點擊數相同，再比較下單數；下單數再相同，就比較支付數。
        // CategoryCountInfo需要能夠比較大小 @Override compareTo
        JavaRDD<CategoryCountInfo> result = categoryCountInfoJavaPairRDD
                .values() // 取出 CategoryCountInfo
                .sortBy(
                        new Function<CategoryCountInfo, CategoryCountInfo>() {
                            @Override
                            public CategoryCountInfo call(CategoryCountInfo v1) throws Exception {
                                return v1; // 直接用 CategoryCountInfo 自己的 compareTo
                            }
                        },
                        false, // false = 降序（熱門在前）
                        2      // 分區數
                );

//        result.collect().forEach(System.out::println);

        //取前10條
        List<CategoryCountInfo> take = result.take(10);

        System.out.println("---------");
        take.forEach(System.out::println);
        System.out.println("---------");

        javaSparkContext.close();

    }


    //轉成物件處理
    public JavaRDD<UserVisitAction> toUserVisitActionRDD(JavaRDD<String> filterRDD ){
        //讀取每行處理
        return filterRDD.map(new Function<String, UserVisitAction>() {
            //讀取每行處理
            @Override
            public UserVisitAction call(String v1) throws Exception {
                String[] split = v1.split("_");
                UserVisitAction userVisitAction = new UserVisitAction();
                userVisitAction.setDate(split[0]);
                userVisitAction.setUserId(Long.valueOf(split[1]));
                userVisitAction.setSessionId(split[2]);
                userVisitAction.setPageId(Long.valueOf(split[3]));
                userVisitAction.setActionTime(split[4]);
                userVisitAction.setSearchKeyword(split[5]);
                userVisitAction.setClickCategoryId(Long.valueOf(split[6]));
                userVisitAction.setClickProductId(Long.valueOf(split[7]));
                userVisitAction.setOrderCategoryIds(split[8]);
                userVisitAction.setOrderProductIds(split[9]);
                userVisitAction.setPayCategoryIds(split[10]);
                userVisitAction.setPayProductIds(split[11]);
                userVisitAction.setCityId(Long.valueOf(split[12]));
                return userVisitAction;
            }
        });
    }


    // 用戶行為記錄實體類
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class UserVisitAction implements Serializable {
        private static final long serialVersionUID = 1L; // 序列化版本號
        private String date;              // 用戶點擊行為的日期
        private Long userId;              // 用戶ID
        private String sessionId;         // Session的ID
        private Long pageId;              // 某個頁面的ID
        private String actionTime;        // 動作的時間點 (時間戳)
        private String searchKeyword;     //收尋關鍵字
        private Long clickCategoryId;     // 點擊的商品品類ID
        private Long clickProductId;      // 點擊的商品ID
        private String orderCategoryIds;  // 一次訂單中所有品類的ID集合
        private String orderProductIds;   // 一次訂單中所有商品的ID集合
        private String payCategoryIds;    // 一次支付中所有品類的ID集合
        private String payProductIds;     // 一次支付中所有商品的ID集合
        private Long cityId;              // 城市ID


    }

    //需要計算 比較的構造 需實現 Comparable
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class CategoryCountInfo implements Serializable, Comparable<CategoryCountInfo> {
        private String categoryId;
        private Long clickCount;
        private Long orderCount;
        private Long payCount;

        @Override
        public int compareTo(CategoryCountInfo o) {
            // 先按照點擊數排名，靠前的就排名高；如果點擊數相同，再比較下單數；下單數再相同，就比較支付數。
            // 小於返回-1,等於返回0,大於返回1
            if (this.getClickCount().equals(o.getClickCount())) {
                if (this.getOrderCount().equals(o.getOrderCount())) {
                    if (this.getPayCount().equals(o.getPayCount())) {
                        return 0;
                    } else {
                        return this.getPayCount() < o.getPayCount() ? -1 : 1;
                    }
                } else {
                    return this.getOrderCount() < o.getOrderCount() ? -1 : 1;
                }
            } else {
                return this.getClickCount() < o.getClickCount() ? -1 : 1;
            }
        }
    }
}
