package com.me.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author maow
 * @create 2020-04-19 11:20
 */
public class FlowBeen implements WritableComparable<FlowBeen> {

    private int order_id; // 订单id号
    private double price; // 价格

    public FlowBeen() {
    }

    public FlowBeen(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }

    public int compareTo(FlowBeen o) {
        int result;

        if (order_id > o.getOrder_id()) {
            result = 1;
        } else if (order_id < o.getOrder_id()) {
            result = -1;
        } else {
            // 价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.price = in.readDouble();
    }
}
