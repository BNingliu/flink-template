package com.ytd.template.model;

import lombok.Data;

@Data
public class JobRunParam {
    /**
     * sql语句目录
     */
    private String sqlPath;


    private Integer parallelism;

}
