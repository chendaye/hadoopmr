package org.example.hadoop.mr.train;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClickWritable implements Writable {
    private String ip = ""; // ip
    private long click = 0; // 点击次数

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getClick() {
        return click;
    }

    public void setClick(Integer click) {
        this.click = click;
    }

    public ClickWritable() {}

    public ClickWritable(String ip, long click) {
        this.ip = ip;
        this.click = click;
    }

    @Override
    public String toString() {
        return "IP+"+this.ip+"\t"+"Clicks="+this.click;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.ip);
        out.writeLong(this.click);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ip = in.readUTF();
        this.click = in.readLong();
    }
}
