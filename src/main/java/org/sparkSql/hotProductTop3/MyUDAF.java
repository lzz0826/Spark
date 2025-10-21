package org.sparkSql.hotProductTop3;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.*;


// 自定義UDAF函數，實現 總點擊數 每個城市的點擊數量
//      1. 創建自定義的【公共】類
//      2. 繼承 org.apache.spark.sql.expressions.Aggregator
//      3. 設定泛型
//          IN : 輸入數據類型
//          BUFF : 緩衝區的數據類型
//          OUT : 輸出數據類型
//     4. 重寫方法 （ 4（計算） + 2(狀態)）
public class MyUDAF extends Aggregator<String, MyBuffer, String>  {


    //  緩衝區的初始化操作
    @Override
    public MyBuffer zero() {
        MyBuffer myBuffer = new MyBuffer();
        myBuffer.setCount(0L);
        myBuffer.setCityMap(new HashMap<>());
        return myBuffer;
    }

    //  將輸入值和緩衝區的數據進行聚合操作
    @Override
    public MyBuffer reduce(MyBuffer b, String a) {
        b.setCount(b.getCount() + 1);
        Map<String, Long> cityMap = b.getCityMap();
        cityMap.put(a, cityMap.getOrDefault(a, 0L) + 1L);
        return b;
    }

    //  合併緩衝區的數據
    @Override
    public MyBuffer merge(MyBuffer b1, MyBuffer b2) {
        b1.setCount(b1.getCount() + b2.getCount());

        Map<String, Long> cityMap1 = b1.getCityMap();
        Map<String, Long> cityMap2 = b2.getCityMap();

        // 把 b2 的內容合併進 b1
        cityMap2.forEach((key, value) ->
                cityMap1.merge(key, value, Long::sum)
        );

        return b1;
    }

    // 計算最終結果
    // 北京21.2%   天津13.2%   其他65.6%
    // 北京/count  天津/count  其他 100-北京-天津
    // 拿到前兩個 剩下其他
    @Override
    public String finish(MyBuffer reduction) {

        StringBuffer ss = new StringBuffer();

        Long total = reduction.getCount();
        Map<String, Long> cityMap = reduction.getCityMap();

        //排序
        List<CityCount> list = new ArrayList<>();


        cityMap.forEach((cityKey, value) -> {
            list.add(new CityCount(cityKey, value));
        });

        Collections.sort(list);

        final CityCount cityCount0 = list.get(0);
        final long pc0 = cityCount0.getCount() * 100 / total; // 10 * 100/20 => 50
        ss.append(cityCount0.getCityName()).append(" ").append(pc0).append("%");


        final CityCount cityCount1 = list.get(1);
        final long pc1 = cityCount1.getCount() * 100 / total; // 10 * 100/20 => 50
        ss.append(cityCount1.getCityName()).append(" ").append(pc1).append("%");

        if ( list.size() > 2 ) {
            ss.append("其他 ").append(100 - pc0 - pc1).append("%");
        }


        return ss.toString();
    }



    //固定寫法 緩衝區的類型
    @Override
    public Encoder<MyBuffer> bufferEncoder() {
        return Encoders.bean(MyBuffer.class);
    }

    //固定寫法 輸出的類型
    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}

