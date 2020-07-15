package com.me.prititionMr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-18 20:45
 */
public class PartitionReducer extends Reducer<Text,FlowBeen,Text,FlowBeen> {
    FlowBeen flowBeen;
    @Override
    protected void reduce(Text key, Iterable<FlowBeen> values, Context context) throws IOException, InterruptedException {
        int upLoadSum = 0;
        int downLoadSum = 0;
        int sumLoadSum = 0;
        for (FlowBeen value : values) {
            upLoadSum += value.getUpLoad();
            downLoadSum += value.getDownLoad();
            sumLoadSum += value.getSumLoad();
        }
        flowBeen = new FlowBeen(upLoadSum,downLoadSum,sumLoadSum);
        context.write(key,flowBeen);
    }
}
