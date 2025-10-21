package org.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

//數據來源 Mysql
public class SparkSourceMySQL {
    public static void main(String[] args) {

        //  構建環境對象
        //      Spark在結構化數據的處理場景中對核心功能，環境進行了封裝
        //      構建SparkSQL的環境對象時，一般採用構建器模式
        //      構建器模式： 構建對象
        final SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkSQL")
                .getOrCreate();


        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","rootpass");

//        json.write()
//                // 寫出模式針對於表格追加覆蓋
//                .mode(SaveMode.Append)
//                .jdbc("jdbc:mysql://127.0.0.1:3306","gmall.testInfo",properties);

        Dataset<Row> jdbc = sparkSession.read()
                .jdbc("jdbc:mysql://127.0.0.1:3306/metastore?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true", "admin_permit", properties);

        jdbc.write()
            .jdbc("jdbc:mysql://127.0.0.1:3306/metastore?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true", "admin_permit_test", properties);

        jdbc.show();

        //  釋放資源
        sparkSession.close();

    }
}
