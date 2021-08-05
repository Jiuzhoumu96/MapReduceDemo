package com.lanhuigu.bigdata.mapreduce.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: HeChengyao
 * @date: 2021/8/2 19:35
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // step1. 获取 job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // step2. 设置 jar 包路径
        job.setJarByClass(WordCountDriver.class);
        // step3. 关联 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // step4. 设置 map 输出的 K，V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // step5. 设置最终的 K，V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // step6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\IDEAWorkSpace\\hechengyao\\wcInput.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\IDEAWorkSpace\\hechengyao\\wcOutput.txt"));
        // step7. 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
