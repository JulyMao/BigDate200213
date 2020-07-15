package com.me.prititionMr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-18 20:45
 */
public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息和创建job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.指定本程序jar 包所在路径
        job.setJarByClass(PartitionDriver.class);
        //3.指定mapper和reducer类
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        //4.指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBeen.class);
        //5.指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeen.class);
        //6.指定job原文件的输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\program_test\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\program_test\\output_partition"));

        //Partition配置信息
        job.setPartitionerClass(MyPartition.class);
        job.setNumReduceTasks(5);

        //7.提交job
        job.waitForCompletion(true);
    }
}
