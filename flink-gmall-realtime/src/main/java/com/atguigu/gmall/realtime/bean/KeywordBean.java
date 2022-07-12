package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 关键词统计实体类
 *
 * @author pangzl
 * @create 2022-07-12 11:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;
    // 窗口闭合时间
    private String edt;
    // 关键词来源
    private String source;
    // 关键词
    private String keyword;
    // 关键词出现频次
    private Long keyword_count;
    // 真正统计事件的时间戳
    private Long ts;
}
