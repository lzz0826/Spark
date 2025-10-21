package org.sparkSql;



import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;


// 自定義UDAF函數，實現年齡的平均值
//      1. 創建自定義的【公共】類
//      2. 繼承 org.apache.spark.sql.expressions.Aggregator
//      3. 設定泛型
//          IN : 輸入數據類型
//          BUFF : 緩衝區的數據類型
//          OUT : 輸出數據類型
//     4. 重寫方法 （ 4（計算） + 2(狀態)）
public class MyAvgAgeUDAF extends Aggregator<Long, AvgAgeBuffer, Long>  {


    @Override
    //  緩衝區的初始化操作
    public AvgAgeBuffer zero() {
        return new AvgAgeBuffer(0L, 0L);
    }

    @Override
    //  將輸入的年齡和緩衝區的數據進行聚合操作
    public AvgAgeBuffer reduce(AvgAgeBuffer buffer, Long in) {
        buffer.setTotal(buffer.getTotal() + in);
        buffer.setCnt(buffer.getCnt() + 1);
        return buffer;
    }

    @Override
    //  合併緩衝區的數據
    public AvgAgeBuffer merge(AvgAgeBuffer b1, AvgAgeBuffer b2) {
        b1.setTotal(b1.getTotal() + b2.getTotal());
        b1.setCnt(b1.getCnt() + b2.getCnt());
        return b1;
    }

    @Override
    //  計算最終結果
    public Long finish(AvgAgeBuffer buffer) {
        return buffer.getTotal() / buffer.getCnt();
    }


    //固定寫法 緩衝區的類型
    @Override
    public Encoder<AvgAgeBuffer> bufferEncoder() {
        return Encoders.bean(AvgAgeBuffer.class);
    }

    //固定寫法 輸出的類型
    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }

}
