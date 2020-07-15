package com.me.outputMR;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 12:51
 */
public class MyOutput extends FileOutputFormat<Text, NullWritable> {
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 创建一个RecordWriter
        return new MyRecordWriter(job);
    }
}
