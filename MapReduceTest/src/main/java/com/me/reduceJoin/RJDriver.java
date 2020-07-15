package com.me.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 20:27
 */
public class RJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(RJDriver.class);

        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        job.setMapOutputKeyClass(FlowBeen.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBeen.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\program_test\\inputReduceJoin"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\program_test\\outputReduceJoin"));

        job.waitForCompletion(true);

    }
}
