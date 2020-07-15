package com.me.outputMR;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 12:49
 */
public class OutputDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OutputDriver.class);

        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\program_test\\inputOutput"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\program_test\\output_file2"));

        job.setOutputFormatClass(MyOutput.class);

        job.waitForCompletion(true);
    }
}
