package com.me.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 20:26
 */
public class RJMapper extends Mapper<LongWritable, Text,FlowBeen, NullWritable> {

    FlowBeen flowBeen = new FlowBeen();
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit =  (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if ("order.txt".equals(filename)){
            flowBeen.setId(fields[0]);
            flowBeen.setPid(fields[1]);
            flowBeen.setAmount(Integer.parseInt(fields[2]));
            flowBeen.setPname("");
            flowBeen.setFlag("order");
        } else {
            flowBeen.setPid(fields[0]);
            flowBeen.setPname(fields[1]);
            flowBeen.setAmount(0);
            flowBeen.setId("");
            flowBeen.setFlag("pd");
        }
        context.write(flowBeen,NullWritable.get());
    }
}
