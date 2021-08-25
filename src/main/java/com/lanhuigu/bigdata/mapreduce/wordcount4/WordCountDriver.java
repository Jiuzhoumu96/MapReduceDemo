package com.lanhuigu.bigdata.mapreduce.wordcount4;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: HeChengyao
 * 这个类使用了 Kerberos 但是比较 low
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
    private static Map<String, String> concurrentHashMap = new ConcurrentHashMap<String, String>();
    private static Logger logger = LoggerFactory.getLogger(WordCountDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
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
        // // HDFS的Kerberos认证
        // job.getConfiguration().set("java.security.krb5.conf", "/etc/krb5.conf");
        // // 必须加，不然会报找不到文件系统
        // job.getConfiguration().addResource(new Path("$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
        // job.getConfiguration().addResource(new Path("$HADOOP_HOME/etc/hadoop/core-site.xml"));
        // // 设置conf信息
        // job.getConfiguration().setBoolean("hadoop.security.authorization", true);
        // job.getConfiguration().set("hadoop.security.authentication", "kerberos");
        // try {
        //     // UserGroupInformation.setConfiguration(conf);
        //     UserGroupInformation.setConfiguration(job.getConfiguration());
        //     UserGroupInformation.loginUserFromKeytab(USER_KEY, KEY_TAB_PATH);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
        // logger.info(">>>>>> Kerberos Checked Finsh!  Get HDFS Data !");

        // step6. 设置输入路径和输出路径
        argsDealing(args);
        // kdestroy、klist、kinit -kt /opt/beh/metadata/key/hadoop.keytab hadoop
        String command = commandHead + KEY_TAB_PATH;
        Runtime.getRuntime().exec(command);
        logger.info(">>>>>> Kerberos " + command);
        job.getConfiguration().set(MAPREDUCE_JOB_QUEUE_NAME, QUEUE_NAME);
        for (Map.Entry<String, String> entry : concurrentHashMap.entrySet()) {
            logger.info(">>>>>> 其他参数 " + entry.getKey() + " = " + entry.getValue());
            job.getConfiguration().set(entry.getKey(), entry.getValue());
        }
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
        for (String arg : args) {
            String argKey = arg.substring(0, arg.indexOf("="));
            String argValue = arg.substring(arg.indexOf("=") + 1);
            switch (argKey) {
                case "-Di":
                    INPUT_PATH = argValue;
                    break;
                case "-Do":
                    OUTPUT_PATH = argValue;
                    break;
                case "-Dk":
                    KEY_TAB_PATH = argValue.replace("'", "");
                    break;
                case "-Dq":
                    QUEUE_NAME = argValue;
                    break;
                default:
                    concurrentHashMap.put(argKey, argValue);
                    break;
            }
        }
    }

}
