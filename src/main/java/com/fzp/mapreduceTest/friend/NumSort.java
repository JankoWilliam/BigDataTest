package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 推荐好友列表按照hot值排序，实现二次排序
 */
public class NumSort extends WritableComparator {

    public NumSort(){
        super(FriendSort.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        FriendSort o1 =(FriendSort) a;
        FriendSort o2 =(FriendSort) b;

        int r =o1.getFriend().compareTo(o2.getFriend());
        if(r==0){
            return -Integer.compare(o1.getHot(), o2.getHot());
        }
        return r;
    }
}
