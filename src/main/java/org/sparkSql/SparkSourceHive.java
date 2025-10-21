package org.sparkSql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URI;

//數據來源 Hive(需要有個 Hadoop warehouse)
public class SparkSourceHive {
    public static void main(String[] args) {

        // 設定 Hadoop 訪問用戶
        System.setProperty("HADOOP_USER_NAME", "root");

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        // 取得 Hadoop Configuration
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

        //---------------------- 讓data node 對外訪問 ------------------------------------
        // 客戶端用 hostname 去連 DataNode
        hadoopConf.set("dfs.client.use.datanode.hostname", "true");
        // DataNode 用 hostname 向外宣告
        hadoopConf.set("dfs.datanode.use.datanode.hostname", "true");
        //----------------------------------------------------------


        // 確保 HDFS warehouse 存在
        String warehousePath = "hdfs://127.0.0.1:8020/user/hive/warehouse";
        try {
//            Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
            FileSystem fs = FileSystem.get(new URI(warehousePath), hadoopConf);
            if (!fs.exists(new Path(warehousePath))) {
                System.out.println("HDFS warehouse 不存在，正在創建...");
                fs.mkdirs(new Path(warehousePath));
                System.out.println("已創建 HDFS warehouse: " + warehousePath);
            } else {
                System.out.println("HDFS warehouse 可用: " + warehousePath);
            }


        } catch (Exception e) {
            System.err.println("連接 HDFS 失敗: " + e.getMessage());
            e.printStackTrace();
        }

        try {

            Dataset<Row> showTables = sparkSession.sql("SHOW TABLES");
            showTables.show();

//            // 創建 Hive 表，如果已存在就先刪掉
            sparkSession.sql("DROP TABLE IF EXISTS user_info");
            sparkSession.sql("CREATE TABLE user_info3(name STRING, age INT)");
//
//            // 插入多條數據
            sparkSession.sql("INSERT INTO user_info VALUES('zhangsan', 10)");
            sparkSession.sql("INSERT INTO user_info VALUES('lisi', 20)");
            sparkSession.sql("INSERT INTO user_info VALUES('wangwu', 30)");

            // 查詢數據
            Dataset<Row> result = sparkSession.sql("SELECT * FROM user_info");
            result.show();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 釋放資源
            sparkSession.close();
        }
    }
}
