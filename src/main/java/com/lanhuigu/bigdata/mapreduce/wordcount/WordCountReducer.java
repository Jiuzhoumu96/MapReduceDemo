package com.lanhuigu.bigdata.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        // atguigu,(1,1)
        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        // 写出
        System.out.println(">>>>>> key = " + key + ",outV= " + outV);
        context.write(key, outV);
    }
}
