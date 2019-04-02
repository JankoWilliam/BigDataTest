package com.fzp.mapreduceTest.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendJob {

    public static void main(String[] args) throws IOException {
        boolean b = jobOne();
        if (b) {
            jobtwo();
        }
    }

    public static boolean jobOne () throws IOException {

        boolean flag = false;

        Configuration conf;
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node02:8020");
        conf.set("yarn.resourcemanager.hostname","node02:8088");

        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendJob.class);
        job.setJobName("friend01");

        job.setMapperClass(Mapper01.class);
        job.setReducerClass(Reducer01.class);

        job.setOutputKeyClass(FoF.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job,new Path ("/friend/input/data.txt"));

        Path output = new Path("/friend/output/001");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output);
        }
        FileOutputFormat.setOutputPath(job,output);

        try {
            flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("job1 success...");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            return flag;
        }

    }

    public static void jobtwo() throws IOException {
        boolean flag = false;

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node02:8020");
        conf.set("yarn.resourcemanager.hostname","node02:8088");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendJob.class);
        job.setJobName("fof two job");

        job.setMapperClass(Mapper02.class);
        job.setReducerClass(Reducer02.class);
        job.setMapOutputKeyClass(FriendSort.class);
        job.setMapOutputValueClass(Text.class);
//        job.setPartitionerClass();

        job.setSortComparatorClass(NumSort.class); //sort类
        job.setGroupingComparatorClass(FriendGroup.class); //group类



        FileInputFormat.addInputPath(job, new Path("/friend/output/001"));

        Path output = new Path("/friend/output/002");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);

        try {
            flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("job2 success...");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
