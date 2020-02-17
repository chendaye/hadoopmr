package org.example.hadoop.mr.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义（hadoop）复杂类型
 *
 * 1)按照hadoop规范 实现 Writable 接口
 * 2)按照hadoop规范  实现 write  和 readFields 方法
 * 3) 定义一个默认的构造方法
 */
public class Access implements Writable {
    private String phone;  // 手机号
    private long up;    // 上行流量
    private long down;  // 下行流量
    private long sum;   // 流量和

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }


    @Override
    public String toString() {
        return "Access{" +
                "phone='" + phone + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }

    /**
     * 默认的构造方法
     */
    public Access() {}

    /**
     * 构造函数
     * @param phone
     * @param up
     * @param down
     */
    public Access(String phone, long up, long down){
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 把内容写出去
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 从流里面读取数据
        this.phone = dataInput.readUTF();
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();
    }
}
