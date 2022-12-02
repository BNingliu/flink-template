package com.ytd.template.api;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @program: tms-flink
 * @description:
 * @author: liuningbo
 * @create: 2021/09/05 12:28
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(ArrayUtils.toString(args));
        if(args.length>0){
            for (String arg : args) {
                System.out.println(arg);
            }
        }
        DataStreamSource<String> source = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                String[] words = {"spark", "flink", "hadoop", "hdfs", "yarn"};
                Random random = new Random();
                while (true) {
                    ctx.collect(words[random.nextInt(words.length)]);
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {

            }
        });

        source.print();
        env.execute();
    }
}