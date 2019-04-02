package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Reducer02 extends Reducer<FriendSort, Text,Text,Text> {

    @Override
    protected void reduce(FriendSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("-------------------------");
        String msg = "";
        for (Text v : values){
            msg += v.toString()+",";
        }
        System.out.println(key);
        context.write(new Text(key.getFriend()),new Text(msg));
    }
}
