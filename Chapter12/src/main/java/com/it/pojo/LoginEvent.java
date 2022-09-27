package com.it.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2022-05-21 12:25
 * @description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginEvent {

    public String userId;
    public String ipAddress;
    public String eventType;
    public Long timestamp;

}
