package com.me.reduceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author maow
 * @create 2020-04-19 20:27
 */
public class RJReducer extends Reducer<FlowBeen, NullWritable,FlowBeen,NullWritable> {
    @Override
    protected void reduce(FlowBeen key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //第一条数据来自pd，之后全部来自order,因为在been中做了二级排序
        Iterator<NullWritable> iterator = values.iterator();

        //通过第一条数据获取pname
        iterator.next();
        String pname = key.getPname();
        System.out.println(pname + "===================================");

        //遍历剩下的数据，替换并写出
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }

    }
}
