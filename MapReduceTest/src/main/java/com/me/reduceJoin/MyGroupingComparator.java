package com.me.reduceJoin;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author maow
 * @create 2020-04-19 11:20
 */
public class MyGroupingComparator extends WritableComparator {

    protected MyGroupingComparator(){
        super(FlowBeen.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBeen f1 = (FlowBeen)a;
        FlowBeen f2 = (FlowBeen)b;
        return new Integer(f1.getPid()).compareTo(new Integer(f2.getPid()));
    }
}
