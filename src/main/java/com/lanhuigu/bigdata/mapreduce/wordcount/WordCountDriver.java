package com.lanhuigu.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author: HeChengyao
 * @date: 2021/8/2 19:35
 */

public class WordCountDriver {

    /**
     * kerberos认证配置
     */
    private static final String JAVA_KRB5_DEBUG = "sun.security.krb5.debug";
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String PRINCIPAL = "security.kerberos.login.principal";
    public static final String KEYTAB = "security.kerberos.login.keytab";
    private static Logger logger = LoggerFactory.getLogger(WordCountDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        // step1. 获取 job
        Configuration conf = new Configuration();

        argsDealing(args);
        String krbFile = args[1];
        String userKeyTableFile = args[2];
        final String PRINCIPAL = args[3];

        logger.info(">>>>>> krbFile = " + krbFile);
        logger.info(">>>>>> userKeyTableFile = " + userKeyTableFile);
        logger.info(">>>>>> PRINCIPAL = " + PRINCIPAL);
        LoginUtil.login(PRINCIPAL, userKeyTableFile, krbFile, conf);




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
        // job.getConfiguration().set(MAPREDUCE_JOB_QUEUE_NAME, QUEUE_NAME);
        String INPUT_PATH = args[args.length - 2];
        String OUTPUT_PATH = args[args.length - 1];
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
