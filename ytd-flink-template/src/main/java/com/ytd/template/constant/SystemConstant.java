package com.ytd.template.constant;

import java.util.regex.Pattern;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/03/17 10:25
 */
public class SystemConstant {

    public final static String COMMENT_SYMBOL = "--";

    public final static String SEMICOLON = ";";

    public final static String LINE_FEED = "\n";

    public final static String SPACE = "";

    public final static String VIRGULE = "/";

    public static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;



    public static final String QUERY_JOBID_KEY_WORD = "job-submitted-success:";

    public static final String QUERY_JOBID_KEY_WORD_BACKUP = "Job has been submitted with JobID";



}
