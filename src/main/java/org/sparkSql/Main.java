package org.sparkSql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;

import static org.apache.spark.sql.functions.udaf;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Main {
    public static void main(String[] args) {


        System.out.println("Hello World");

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Spark SQL")
                .getOrCreate();


        // 環境之間的轉換
        //      Core : SparkContext -> SQL : SparkSession
        //new SparkSession( new SparkContext(conf) );
        //  SQL  : SparkSession -> Core : SparkContext
        //final SparkContext sparkContext = sparkSession.sparkContext();
        //sparkContext.parallelize()
        //  SQL  : SparkSession -> Core : JavaSparkContext
//        final SparkContext sparkContext = sparkSession.sparkContext();
//        final JavaSparkContext jsc = new JavaSparkContext(sparkContext);
//        jsc.parallelize(Arrays.asList(1,2,3,4));


        // Spark SQL中對數據模型也進行了封裝 ： RDD -> Dataset
        //      對接文件數據源時，會將文件中的一行數據封裝為Row對象
        //      SparkSQL只需要保證JSON文件中一行數據符合JSON格式即可，無需整個文件符合JSON格式
        final Dataset<Row> ds = sparkSession.read().json("inputData/user.json");
        //final RDD<Row> rdd = ds.rdd();


        // 將數據模型轉換成表，方便SQL的使用
        ds.createOrReplaceTempView("user");


        // 使用SQL文的方式操作數據
        //      將數據模型轉換為二維的結構（行，列），可以通過SQL文進行訪問
        //      視圖：是表的查詢結果集。表可以增加，修改，刪除，查詢。
        //           視圖不能增加，不能修改，不能刪除，只能查詢
        String sql = "select * from user";
        final Dataset<Row> sqlDS = sparkSession.sql(sql);
        // 展示數據模型的效果
//        sqlDS.show();

        //DSL語法 方式查詢
        ds.select("*").where("name = 'tony'").show();

//-------------------------
        //  SparkSQL提供了一種特殊的方式，可以在SQL中增加自定義方法來實現複雜的邏輯
        //  如果想要自定義的方法能夠在SQL中使用，那麼必須在SPark中進行聲明和註冊
        //      register方法需要傳遞3個參數
        //           第一個參數表示SQL中使用的方法名
        //           第二個參數表示邏輯 : IN => OUT (UDF相當於RDD MAP資料轉換 , UDAF相當於RDD的reduce聚合)
        //           第三個參數表示返回的數據類型 : DataType類型數據，需要使用scala語法操作，需要特殊的使用方式。
        //                 scala Object => Java
        //                 StringType$.MODULE$ => case object StringType
        //                 DataTypes.StringType

        //註冊 UDF new UDF1
        sparkSession.udf().register("prefixName", new UDF1<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "Name:" + name;
            }
        }, StringType);

        //這裡的 prefixName 函數要使用 sparkSession.udf().register 像Spark註冊
        String CustomizeSql = "select prefixName(name) from user";
        final Dataset<Row> CustomizeSqlDS = sparkSession.sql(CustomizeSql);
        System.out.println("-------- 前綴: Name: name   --------");
        CustomizeSqlDS.show();
        System.out.println("-------- 前綴: Name: name   --------");




        // 註冊 SparkSQL採用特殊的方式將UDAF轉換成UDF使用
        //      UDAF使用時需要創建自定義聚合對象
        //        udaf方法需要傳遞2個參數
        //             第一個參數表示UDAF對象
        //             第二個參數表示輸入的類型
        sparkSession.udf().register("avgAge", udaf(new MyAvgAgeUDAF(), Encoders.LONG()));

        String CustomizeSql2 = "select avgAge(age) from user";

        final Dataset<Row> CustomizeSqlAvgAge = sparkSession.sql(CustomizeSql2);
        System.out.println("-------- age總和  --------");
        CustomizeSqlAvgAge.show();
        System.out.println("-------- age總和  --------");



//-------------------------
        //轉成物件
        Dataset<User> userDataset = ds.as(Encoders.bean(User.class));

        userDataset.foreach( u ->{
            System.out.println(u.getId());
            System.out.println(u.getAge());
            System.out.println(u.getName());
        });


        sparkSession.close();

    }

}

