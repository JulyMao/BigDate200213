package com.me.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author maow
 * @create 2020-04-14 19:46
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建一个Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联jar
        job.setJarByClass(WordCountDriver.class);
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
        FileInputFormat.setInputPaths(job,new Path("D:/program_test/input"));
        FileOutputFormat.setOutputPath(job,new Path("D:/program_test/output2"));
        //7.提交Job
        job.waitForCompletion(true);
    }
}
