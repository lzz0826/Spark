package org.sparkSql;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;


@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AvgAgeBuffer implements Serializable {
    private Long total;
    private Long cnt;
}
