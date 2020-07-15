package com.me.wordcountbyyarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author maow
 * @create 2020-04-14 19:46
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建一个Job对象
        Configuration conf = new Configuration();
//        // 开启map端输出压缩
//        conf.setBoolean("mapreduce.map.output.compress", true);
//        // 设置map端输出压缩方式
//        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        //设置HDFS NameNode的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:9820");
        // 指定MapReduce运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        // 指定mapreduce可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");

        //指定Yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname","hadoop103");
        //指定运行的队列为hive
        conf.set("mapred.job.queue.name","hive");
        Job job = Job.getInstance(conf);



        //2.关联jar
//        job.setJarByClass(WordCountDriver.class);
        job.setJar("F:\\IEDA_project\\BigDate200213\\MapReduceTest\\target\\MapReduceTest-1.0-SNAPSHOT.jar");
        //3.关联Mapper 和 Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入和输出路径
//        FileInputFormat.setInputPaths(job,new Path("D:/program_test/input"));
//        FileOutputFormat.setOutputPath(job,new Path("D:/program_test/output2"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交Job
        job.waitForCompletion(true);
    }
}
