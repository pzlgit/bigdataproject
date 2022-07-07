package com.atguigu.gmall.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 员工实体
 *
 * @author pangzl
 * @create 2022-07-07 18:10
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Emp {

    private int empno;

    private String ename;

    private int deptno;

    private Long ts;

}