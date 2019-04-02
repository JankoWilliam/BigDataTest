package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.Text;

public class FoF extends Text {

    public  FoF () {
        super();
    }

    public FoF(String friend1,String friend2) {
        set(getof(friend1,friend2));
    }

    private String getof(String friend1,String friend2) {
        int c = friend1.compareTo(friend2);
        if (c > 0) {
            return friend2+"\t"+friend1;
        }
        return friend1+"\t"+friend2;
    }
}
