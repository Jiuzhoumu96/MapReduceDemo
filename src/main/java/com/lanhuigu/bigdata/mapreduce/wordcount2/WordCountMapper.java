package com.lanhuigu.bigdata.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author: HeChengyao
 * @date: 2021/8/2 19:35
 */

/**
 * KEYIN, map阶段输入的key的类型：LongWritable
 * VALUEIN, map阶段输入的value类型：Text
 * KEYOUT, map阶段输出的key类型：Text
 * VALUEOUT, map阶段输出的value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static Logger logger = LoggerFactory.getLogger(WordCountMapper.class);

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(key);
        logger.info(">>>>>> WordCountMapper key =" + key.toString());
        //1. 获取一行
        String line = value.toString();

        //2.切割
        String[] words = line.split(" ");

        //3. 循环写出
        for (String word : words) {
            //封装outK
            outK.set(word);
            logger.info(">>>>>> WordCountMapper outK =" + outK.toString());
            logger.info(">>>>>> WordCountMapper outV =" + outV.toString());
            //写出
            context.write(outK, outV);
        }

    }
}
