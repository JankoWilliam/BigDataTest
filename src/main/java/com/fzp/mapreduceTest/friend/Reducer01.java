package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class Reducer01 extends Reducer<FoF, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(FoF key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        System.out.println("reduce...");
        int sum = 0;
        boolean temp = true ;
        for (IntWritable v : values){
            if (v.get() == 0) {
                temp = false;
                break;
            }
            sum += v.get();
        }
        if (temp) {
            String result = StringUtils.split(key.toString(), '\t')[0]+" "+StringUtils.split(key.toString(), '\t')[1]+" "+sum;
            System.out.println(result);
            context.write(new Text(result),NullWritable.get()); //输出潜在好友对，和次数
        }
    }
}
