package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class Mapper01 extends Mapper<LongWritable, Text,FoF, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("map...");
        String[] friends = value.toString().split(" ");
        if (friends.length > 0){
            for (int i = 1;i<friends.length;i++){
                context.write(new FoF(friends[0],friends[i]),new IntWritable(0));// 已经是好友输出0
                for (int j = i+1; j<friends.length;j++){
                    context.write(new FoF(friends[i],friends[j]),new IntWritable(1));//好友之间的用户互相配对输出1
                }
            }
        } else {
            context.write(new FoF(friends[0],friends[0]),new IntWritable(0));
        }

    }

}
