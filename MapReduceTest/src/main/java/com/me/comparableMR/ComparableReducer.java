package com.me.comparableMR;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.checkerframework.checker.units.qual.K;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-18 21:51
 */
public class ComparableReducer extends Reducer<FlowBeen,Text,Text,FlowBeen> {
    FlowBeen flowBeen;
    @Override
    protected void reduce(FlowBeen key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int upLoadSum = 0;
        int downLoadSum = 0;
        int sumLoadSum = 0;
        for (Text value : values) {
            context.write(value,key);
//            upLoadSum += value.getUpLoad();
//            downLoadSum += value.getDownLoad();
//            sumLoadSum += value.getSumLoad();
        }
//        flowBeen = new FlowBeen(upLoadSum,downLoadSum,sumLoadSum);
//        context.write(flowBeen,key);
    }
}
