package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    public Integer vc;
    private Long ts;
}
