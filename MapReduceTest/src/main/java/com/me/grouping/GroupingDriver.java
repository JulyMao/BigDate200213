package com.me.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 11:19
 */
public class GroupingDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(GroupingDriver.class);

        job.setMapperClass(GroupingMapper.class);
        job.setReducerClass(GroupingReducer.class);

        job.setMapOutputKeyClass(FlowBeen.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBeen.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\program_test\\inputGrouping"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\program_test\\output_grouping1"));

        job.setGroupingComparatorClass(MyGroupingComparator.class);

        job.waitForCompletion(true);
    }
}
