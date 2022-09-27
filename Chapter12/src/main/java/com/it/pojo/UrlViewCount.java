package com.it.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2022-05-06 11:03
 * @description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UrlViewCount {

    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

}
