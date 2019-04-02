package com.fzp;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class EasyHdfsTest {

    private static Configuration config = new Configuration();
    private static FileSystem fs;

    public static void main(String[] args) throws IOException {
        // 配置
        config.set("fs.defaultFS","hdfs://node01:8020");
        fs = FileSystem.get(config);
        // 获取block信息，指定偏移量读取
//        block();
        //
        // 从hdfs下载文件
//        downloadFile();
        // 上传文件
//        uploacdFile();
        // 创建目录
//        mkAndDelDir();
        // 列举文件状态
//        listFileStatus();
        // 合并小文件上传
//        seqFile();
    }

    public static void seqFile () throws IOException {
        File files = new File("D:/hdfsData/");
        Path path = new Path("/data/seqData");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs,config,path, Text.class,Text.class, SequenceFile.CompressionType.NONE);
        for (File ff : files.listFiles()) {
            writer.append(new Text(ff.getName()),new Text(FileUtils.readFileToString(ff,"utf-8")));
        }
        System.out.println("合并上传小文件成功");
    }

    public static void listFileStatus () throws IOException {
        // 指定目录
        Path path = new Path("/data");
        FileStatus[] fileStatus = fs.listStatus(path);
        for (int i = 0; i < fileStatus.length; i++) {
            FileStatus status = fileStatus[i];
            System.out.println(
                    status.getOwner()+"\t"+
                            status.getAccessTime()+"\t"+
                            status.getModificationTime()+"\t"+
                            status.getBlockSize()/1024/1024+"MB \t"+
                            status.getPath()
            );

        }
    }

    public static void mkAndDelDir () throws IOException {
        // 指定需要创建的目录
        Path path = new Path("/data/test");
        if (fs.exists(path)){
            fs.delete(path,true);
            System.out.println("目录已存在，删除...");
        }
        fs.mkdirs(path);
        System.out.println("目录创建成功");
    }

    public static void uploacdFile () throws IOException {
        // 本地源文件
        File file = new File("D:/aaa.pdf");
        // 目标文件
        Path path = new Path("/data/bbb.pdf");
        FSDataOutputStream output = fs.create(path);
        // 上传文件到fdfs
        IOUtils.copyBytes(new FileInputStream(file),output,config);
        System.out.println("上传成功");
    }

    public static void downloadFile () throws IOException {
        // 源文件
        Path path = new Path("/data/seqData");
        // 目标文件
        File file =  new File("D:/zzz");
        FSDataInputStream input = fs.open(path);
        // 读数据写入文件
        IOUtils.copyBytes(input,new FileOutputStream(file),config);
        System.out.println("下载成功");

    }

    public static void block() throws IOException {
        // 数据源文件
        Path path = new Path("/data/file.pdf");
        // 创建读取数据对象
        FSDataInputStream input = fs.open(path);
        System.out.println((char) input.readByte());
        System.out.println((char) input.readByte());
        System.out.println((char) input.readByte());
        System.out.println((char) input.readByte());
        System.out.println((char) input.readByte());
        // 指定从哪个offset的位置偏移量读
        input.seek(101);
        System.out.println((char) input.readByte());
        System.out.println((char) input.readByte());
        input.seek(102);
        System.out.println((char) input.readByte());
        System.out.println((char) input.readByte());


        FileStatus fileStatus = fs.getFileStatus(path);
        // 获取block的location信息
        BlockLocation[] blk = fs.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
        for (BlockLocation bb : blk)
            System.out.println("++" + bb);
    }
}
