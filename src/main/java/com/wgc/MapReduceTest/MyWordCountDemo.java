package com.wgc.MapReduceTest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author wanggc
 * @date 2019/09/12 星期四 11:45
 * 这是MapReduce测试的wordcount程序
 * 提交到集群yarn执行的脚本是：
 * hadoop jar com.wgc.MapReduceTest-1.0-SNAPSHOT.jar  /user/eda/input /user/eda/output6
 * /user/eda/output6输出目录不能存在，否则会报错
 */

public class MyWordCountDemo {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        /**
         * 提交到集群yarn执行不必配置Kerberos代码
         * */
/*        // hdfs HA模式的配置
        // 配置Kerberos
        System.setProperty("java.security.krb5.conf", "krb5.conf");
        //System.setProperty("hadoop.home.dir", "E:\\BigData\\Linux\\hadoop-2.5.0");
        //System.setProperty("HADOOP_MAPRED_HOME", "E:\\BigData\\Linux\\hadoop-2.5.0");
        conf.set("hadoop.security.authentication", "Kerberos");
        //hdfs
        conf.set("dfs.datanode.kerberos.principal", "hdfs/_HOST@MYCDH");
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@MYCDH");
        //yarn
        conf.set("yarn.resourcemanager.principal","yarn/_HOST@MYCDH");
        conf.set("yarn.nodemanager.principal","yarn/_HOST@MYCDH");

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("eda@MYCDH", "eda.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        //在 main 函数中通过 Job 类设置 Hadoop MapReduce 程序运行时的环境变量
        //创建一个新Job
        Job job = Job.getInstance(conf,"myWordCount");
        //Set the Jar by finding where a given class came from.
        job.setJarByClass(MyWordCount.class);
        //为job设置 Mapper 类
        job.setMapperClass(MyWordCount.MyMapper.class);
        //为job设置 combiner 类
        job.setCombinerClass(MyWordCount.MyReducer.class);
        //为job设置 Reducer类
        job.setReducerClass(MyWordCount.MyReducer.class);
        //设置job输出 key 的类型
        job.setOutputKeyClass(Text.class);
        //设置job输出 value 的类型
        job.setOutputValueClass(IntWritable.class);
        //设置输入文件路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出的文件路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
/*
        //设置输入文件路径
        FileInputFormat.addInputPath(job,new Path("hdfs://134.64.14.230:8020/user/eda/input"));
        //设置输出的文件路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://134.64.14.230:8020/user/eda/output2"));
        */
        //提交作业，然后轮询进度直到作业完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
