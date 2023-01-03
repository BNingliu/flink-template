package com.ytd.template.api;

import scala.tools.nsc.backend.icode.Members;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Date;

/**
 * @program: ytd-flink-template
 * @description:
 * @author: liuningbo
 * @create: 2022/12/16 21:38
 */
public class Demo {



    public static void main(String[] args) throws ParseException {
//        \"create_time\":1671225406000,
        LocalDateTime parse = LocalDateTime.parse("2022-12-13 21:32:37"
                , DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime now1 = LocalDateTime.now();
        Instant instant = Instant.ofEpochMilli(1671225406000L);
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
//        LocalDateTime start=LocalDateTime.of(2018,4,1,10,10,10);
//        LocalDateTime end=LocalDateTime.of(2018,3,28,10,10,11);
        Duration duration= Duration.between(localDateTime,now1);
        System.out.println(duration.toDays());

//

    }
}
