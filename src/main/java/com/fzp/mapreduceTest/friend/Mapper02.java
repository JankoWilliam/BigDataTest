package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Mapper02 extends Mapper<LongWritable, Text,FriendSort,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        String friend1 = words[0];
        String friend2 = words[1];
        int hot = Integer.parseInt(words[2]);
        // 输出用户和好友推荐系数
        context.write(new FriendSort(friend1,hot),new Text(friend2+":"+hot));
        // 好友关系是相互的
        context.write(new FriendSort(friend2,hot),new Text(friend1+":"+hot));
    }
}
