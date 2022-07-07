package com.atguigu.gmall.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 部门实体
 *
 * @author pangzl
 * @create 2022-07-07 18:11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dept {

    private int deptno;

    private String dname;

    private Long ts;
}