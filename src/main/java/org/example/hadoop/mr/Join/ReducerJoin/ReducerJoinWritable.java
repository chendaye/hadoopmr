package org.example.hadoop.mr.Join.ReducerJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReducerJoinWritable implements Writable {
    private int pid;
    private long amount;
    private String company;
    private String flag;


    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public ReducerJoinWritable() { }

    public ReducerJoinWritable(int pid, long amount, String company, String flag){
        this.pid = pid;
        this.amount = amount;
        this.company = company;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.pid);
        dataOutput.writeLong(this.amount);
        dataOutput.writeUTF(this.company);
        dataOutput.writeUTF(this.flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pid = dataInput.readInt();
        this.amount = dataInput.readLong();
        this.company = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }
}
