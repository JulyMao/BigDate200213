package com.me.outputMR;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 12:59
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {


    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public MyRecordWriter(TaskAttemptContext job) {

        // 1 获取文件系统
        FileSystem fs;

        try {
            fs = FileSystem.get(job.getConfiguration());

            // 2 创建输出文件路径
            Path atguiguPath = new Path("D:\\program_test\\output_file2\\log.txt");
            Path otherPath = new Path("D:\\program_test\\output_file2\\other.txt");

            // 3 创建输出流
            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("atguigu")){
            atguiguOut.write((key.toString() + "\n\r").getBytes());
        }else {
            otherOut.write((key.toString() + "\n\r").getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
//        atguiguOut.close();
//        otherOut.close();
    }
}
