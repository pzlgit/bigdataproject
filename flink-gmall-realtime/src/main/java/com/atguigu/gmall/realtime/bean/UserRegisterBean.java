package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 用户域用户注册实体类
 *
 * @author pangzl
 * @create 2022-07-13 11:50
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}