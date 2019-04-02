package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendSort implements WritableComparable<FriendSort> {

    private String friend;
    private int hot;

    public String getFriend() {
        return friend;
    }

    public void setFriend(String friend) {
        this.friend = friend;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    public FriendSort() {
        super();
    }

    public FriendSort(String friend, int hot) {
        this.friend = friend;
        this.hot = hot;
    }
    // 按hot值进行排序
    public int compareTo(FriendSort newFriend) {

        int c = friend.compareTo(newFriend.getFriend());
        int e = -Integer.compare(hot, newFriend.getHot());
        if (c==0) {
            return e;
        }
        return c;
    }
    // 序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(friend);
        dataOutput.writeInt(hot);
    }
    // 反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.friend = dataInput.readUTF();
        this.hot = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "FriendSort{" +
                "friend='" + friend + '\'' +
                ", hot=" + hot +
                '}';
    }
}
