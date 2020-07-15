package com.me.prititionMr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-18 20:41
 */
public class PartitionMapper extends Mapper<LongWritable,Text, Text, FlowBeen> {
    FlowBeen flowBeen = new FlowBeen();
    Text outK = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] splits = s.split("\t");
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        outK.set(splits[1]);
        flowBeen.setUpLoad(Long.parseLong(splits[splits.length - 3]));
        flowBeen.setDownLoad(Long.parseLong(splits[splits.length - 2]));
        flowBeen.setSumLoad();
        context.write(outK,flowBeen);
    }
}
