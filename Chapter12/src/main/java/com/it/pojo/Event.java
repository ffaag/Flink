package com.it.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2022-04-28 15:48
 * @description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {

    public String user;
    public String url;
    public Long timestamp;

}
