package org.sparkSql.hotProductTop3;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;


public class SparkUtil {

    // 類別層級變數，確保全域唯一
    private static volatile SparkSession sparkSession = null;

    public static SparkSession getHadoopConnect() {
        if (sparkSession == null) {
            synchronized (SparkUtil.class) {

                // 在編碼前，設定Hadoop的訪問用戶
                System.setProperty("HADOOP_USER_NAME","root");

                if (sparkSession == null) {
                    sparkSession = SparkSession
                            .builder()
                            .enableHiveSupport()
                            .master("local[*]")
                            .appName("SparkSQL")
                            .getOrCreate();

                    // 取得 Hadoop Configuration
                    Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

                    //---------------------- 讓data node 對外訪問 ------------------------------------
                    // 客戶端用 hostname 去連 DataNode
                    hadoopConf.set("dfs.client.use.datanode.hostname", "true");
                    // DataNode 用 hostname 向外宣告
                    hadoopConf.set("dfs.datanode.use.datanode.hostname", "true");
                    //----------------------------------------------------------

                }
            }
        }
        return sparkSession;
    }
}



