package com.me.mrphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-15 11:33
 */
public class DriverFlow {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建job对象
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof);
        //2.指定jar类
        job.setJarByClass(DriverFlow.class);
        //3.指定Mapper和Reducer类
        job.setMapperClass(MapperFlow.class);
        job.setReducerClass(ReducerFlow.class);
        //4.指定指定Mapper输出的k 和 v
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.指定最终输出的k 和 v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.指定输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\program_test\\input\\"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\program_test\\output_phone_data5"));
        //7.提交job
        job.waitForCompletion(true);
    }
}
