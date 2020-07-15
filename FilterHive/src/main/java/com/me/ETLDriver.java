package com.me;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author maow
 * @create 2020-04-29 19:41
 */
public class ETLDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("D:\\尚硅谷\\04_hadoop生态\\07_尚硅谷大数据技术之Hive\\07_尚硅谷大数据技术之Hive\\2.资料\\03_data\\guliVideo\\video\\2008\\0222"));
//        FileOutputFormat.setOutputPath(job,new Path("D:\\output"));

        job.waitForCompletion(true);
    }
}
