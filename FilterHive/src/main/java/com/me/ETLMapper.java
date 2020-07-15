package com.me;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-29 19:41
 */
public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text outK = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        System.out.println(s);
        String s1 = ETLUtil.filterString(s);
        if (s1 != null){
            outK.set(s1);
            context.write(outK,NullWritable.get());
        }
    }
}
