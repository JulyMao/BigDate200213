package com.me.mrphone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-15 11:33
 */
public class ReducerFlow extends Reducer<Text,FlowBean,Text, FlowBean> {
    private FlowBean flowBean;
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        long sumFlow = 0;
        for (FlowBean fb : values){
            upFlow += fb.getUpFlow();
            downFlow += fb.getDownFlow();
            sumFlow += fb.getSumFlow();
        }
        flowBean = new FlowBean(upFlow,downFlow,sumFlow);
        context.write(key,flowBean);
    }
}
