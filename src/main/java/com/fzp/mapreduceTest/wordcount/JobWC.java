package com.fzp.mapreduceTest.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class JobWC {

    public static void main(String[] args) throws IOException {

        //1.配置变量
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node01:8020");
        conf.set("yarn.resourcemanager.hostname","node02:8088");

        //2.配置Job任务的相关参数
        Job job = Job.getInstance(conf);
        job.setJarByClass(JobWC.class);
        job.setJobName("wc");

        job.setMapperClass(MapperWC.class);
        job.setReducerClass(ReducerWC.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3.读取hdfs上的数据文件
        FileInputFormat.addInputPaths(job,"/data/file.pdf");

        //4.输出结果到指定位置
        Path path = new Path("/data/output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)){
            fs.delete(path);
        }
        FileOutputFormat.setOutputPath(job,path);

        //5.结束
        boolean f ;

        try {
            f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job success !");
            } else {
                System.out.println("..............");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

}
