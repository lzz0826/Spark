package org.sparkSql.hotProductTop3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;


@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString

public class MyBuffer implements Serializable {
    //每筆(總數)
    private Long count;

    //記錄城市的數量
    private Map<String, Long> cityMap = null;

}