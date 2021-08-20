package com.lanhuigu.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author: HeChengyao
 * @date: 2021/8/2 19:35
 */

public class WordCountDriver {

    // private static String USER_KEY = "hadoop"; //用户key
    private static final String commandHead = "kinit -kt ";
    private static String KEY_TAB_PATH = "/opt/beh/metadata/key/hadoop.keytab";// keytab文件
    private static String INPUT_PATH = "hdfs://beh001/tmp/wcInput.txt"; // 输入路径
    private static String OUTPUT_PATH = "hdfs://beh001/tmp/output/mapReduceOutput"; // 输出路径
    private static final String MAPREDUCE_JOB_QUEUE_NAME = "mapreduce.job.queuename";
    private static String QUEUE_NAME = "default";
    private static Logger logger = Logger.getLogger(WordCountDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        argsDealing(args);
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


        // 设置队列
        job.getConfiguration().set(MAPREDUCE_JOB_QUEUE_NAME, QUEUE_NAME);

        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // step7. 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static void argsDealing(String[] args) {

        int argsLen = args.length;
        logger.info(">>>>>> args.length = " + argsLen);
        for (int i = 0; i < argsLen; i++) {
            logger.info(">>>>>> args[" + i + "] = " + args[i]);
        }
        // 以"-D"开头的参数会被筛选到过滤器中来，然后针对过滤器中的参数进行字符串操作
        Arrays.stream(args).filter(arg -> arg.startsWith("-D")).forEach(arg -> {
            String key = arg.substring(2, arg.indexOf("="));
            String value = arg.substring(arg.indexOf("=") + 1);
            logger.info(">>>>>> key = " + key + ", value = " + value);
            System.setProperty(key, value);
        });
        // 参数的个数少于两个会提示参数不正确，并退出程序。
        if (2 > args.length) {
            logger.error("Invalid params number: " + args.length);
            System.exit(1);
        }

    }

}
