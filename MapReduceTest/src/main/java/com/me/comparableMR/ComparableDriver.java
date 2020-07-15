package com.me.comparableMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-18 21:51
 */
public class ComparableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置文件和创建job对象
        Configuration cnf = new Configuration();
        Job job = Job.getInstance(cnf);
        //2.指定本程序jar包所在路径
        job.setJarByClass(ComparableDriver.class);
        //3.指定mapper和reducer类
        job.setMapperClass(ComparableMapper.class);
        job.setReducerClass(ComparableReducer.class);
        //4.指定mapper输出的kv的类型
        job.setMapOutputKeyClass(FlowBeen.class);
        job.setMapOutputValueClass(Text.class);
        //5.指定最终输出的kv的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeen.class);
        //6.指定文件输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\program_test\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\program_test\\output_comparable3"));
        //7.提交job
        job.waitForCompletion(true);
    }
}
