package com.me.mrphone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-15 11:33
 */
public class MapperFlow extends Mapper<LongWritable,Text,Text,FlowBean> {
    Text outK = new Text();
    FlowBean fb = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str= value.toString();
        String[] split = str.split("\t");
        outK.set(split[2]);
        fb.setUpFlow(Long.parseLong(split[split.length - 3]));
        fb.setDownFlow(Long.parseLong(split[split.length - 2]));
        fb.setSumFlow();
        context.write(outK,fb);
    }
}
