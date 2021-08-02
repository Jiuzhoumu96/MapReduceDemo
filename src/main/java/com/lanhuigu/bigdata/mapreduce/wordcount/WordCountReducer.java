package com.lanhuigu.bigdata.mapreduce.wordcount;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author: HeChengyao
 * @date: 2021/8/2 19:35
 */

/**
 * KEYIN, reduce阶段输入的key的类型：Text
 * VALUEIN, reduce阶段输入的value的类型：IntWritable
 * KEYOUT, reduce阶段输出的key类型：Text
 * VALUEOUT, reduce阶段输出的value类型：IntWritable
 */
public class WordCountReducer extends Reducer {
}
