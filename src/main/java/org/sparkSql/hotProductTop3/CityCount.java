package org.sparkSql.hotProductTop3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;


@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class CityCount implements Serializable , Comparable<CityCount> {

    private String cityName;
    private Long count;

    @Override
    public int compareTo(@NotNull CityCount o) {
        if (this.count < o.count) {
            return -1;
        }else if (this.count > o.count) {
            return 1;
        }
        return 0;
    }
}
