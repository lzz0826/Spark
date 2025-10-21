package org.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

//數據來源 CSV
public class SourceCSV {
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

        // 32 UUID String
        //  CSV文件就是將數據採用逗號分隔的數據文件
        final Dataset<Row> csv = sparkSession.read()
                .option("header", "true") // 配置
//                .option("sep","_") // 配置：\t => tsv, csv
                .csv("inputData/user.csv");

        // org.apache.spark.sql.AnalysisException : bigdata-bj-classes231226/output already exists.

        //  如果輸出目的地已經存在，那麼SparkSQL默認會發生錯誤，如果不希望發生錯誤，那麼就需要修改配置：保存模式
        //      SaveMode.Append : 追加
        //      SaveMode.Overwrite : 覆蓋
        //      SaveMode.ErrorIfExists : 存在就報錯
        //      SaveMode.Ignore : 忽略
        csv.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true") // 配置
                .csv("output");

        // select avg(_c2) from user


        //  釋放資源
        sparkSession.close();

    }
}
