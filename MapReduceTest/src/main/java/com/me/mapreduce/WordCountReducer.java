package com.me.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-14 19:46
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //定义写出的v
    IntWritable outV = new IntWritable();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.迭代values，将当前key对应的所有的value叠加
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        //2.写出
        //将累加后的结果sum封装倒outV中
        outV.set(sum);
        context.write(key,outV);
    }
}
