package com.me.prititionMr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author maow
 * @create 2020-04-18 20:40
 */
public class MyPartition extends Partitioner<Text,FlowBeen> {
    public int getPartition(Text text, FlowBeen flowBeen, int numPartitions) {
        String s = text.toString();
        int partition = 4;
        if (s.startsWith("136")){
            partition = 0;
        }else if (s.startsWith("137")){
            partition = 1;
        }else if (s.startsWith("138")){
            partition = 3;
        }
        return partition;
    }
}
