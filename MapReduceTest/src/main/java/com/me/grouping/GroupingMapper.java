package com.me.grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 11:19
 */
public class GroupingMapper extends Mapper<LongWritable, Text,FlowBeen, NullWritable> {
    FlowBeen flowBeen = new FlowBeen();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] splits = s.split("\t");
        //0000001	Pdt_01	222.8
        flowBeen.setOrder_id(Integer.parseInt(splits[0]));
        flowBeen.setPrice(Double.parseDouble(splits[2]));
        context.write(flowBeen,NullWritable.get());
    }
}
