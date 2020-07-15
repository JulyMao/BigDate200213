package com.me.prititionMr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-18 20:48
 */
public class FlowBeen implements Writable {
    private long upLoad;
    private long downLoad;
    private long sumLoad;

    public FlowBeen() {
    }

    public FlowBeen(long upLoad, long downLoad, long sumLoad) {
        this.upLoad = upLoad;
        this.downLoad = downLoad;
        this.sumLoad = sumLoad;
    }

    @Override
    public String toString() {
        return upLoad + "\t" + downLoad + "\t" + sumLoad;
    }

    public long getUpLoad() {
        return upLoad;
    }

    public void setUpLoad(long upLoad) {
        this.upLoad = upLoad;
    }

    public long getDownLoad() {
        return downLoad;
    }

    public void setDownLoad(long downLoad) {
        this.downLoad = downLoad;
    }

    public long getSumLoad() {
        return sumLoad;
    }

    public void setSumLoad(long sumLoad) {
        this.sumLoad = sumLoad;
    }
    public void setSumLoad(){
        this.sumLoad = this.upLoad + this.downLoad;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(upLoad);
        out.writeLong(downLoad);
        out.writeLong(sumLoad);
    }

    public void readFields(DataInput in) throws IOException {
        this.upLoad = in.readLong();
        this.downLoad = in.readLong();
        this.sumLoad = in.readLong();
    }
}
