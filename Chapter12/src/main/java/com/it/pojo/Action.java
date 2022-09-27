package com.it.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2022-05-13 13:03
 * @description
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Action {

    public String userId;
    public String action;

}
