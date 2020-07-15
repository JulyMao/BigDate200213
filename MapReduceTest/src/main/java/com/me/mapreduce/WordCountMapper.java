package com.me.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-14 19:45
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //定义输出的v
    IntWritable outV = new IntWritable(1);
    //定义输出的k
    Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将输入的一行数据转换成字符串类型
        String line = value.toString();
        //2.使用空格切分数据
        String[] split = line.split(" ");
        //3.迭代split数组，将每个单词处理成kv，写出去
        for (String s:split){
            //将当前单词设置到outK中
            outK.set(s);

            //写出
            context.write(outK,outV);
        }
    }
}
