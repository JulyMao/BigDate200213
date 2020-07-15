package com.me.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 11:19
 */
public class GroupingReducer extends Reducer<FlowBeen, NullWritable, FlowBeen, NullWritable> {
    @Override
    protected void reduce(FlowBeen key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
