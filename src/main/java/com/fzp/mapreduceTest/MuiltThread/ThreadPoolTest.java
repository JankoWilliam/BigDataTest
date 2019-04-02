package com.fzp.mapreduceTest.MuiltThread;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

public class ThreadPoolTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

//        ExecutorService executor = Executors.newFixedThreadPool(2);
//
//        //创建一个Callable，5秒后返回String类型
//        Callable myCallable = new Callable() {
//            @Override
//            public String call() throws Exception {
//                Thread.sleep(5000);
//                System.out.println("calld方法执行了");
//                return "call方法返回值";
//            }
//        };
//        //创建一个Callable，3秒后返回String类型
//        Callable myCallable2 = new Callable() {
//            @Override
//            public String call() throws Exception {
//                Thread.sleep(3000);
//                System.out.println("calld2方法执行了");
//                return "call2方法返回值";
//            }
//        };
//        System.out.println("提交任务之前 " + getStringDate());
//        Future future = executor.submit(myCallable);
//        Future future2 = executor.submit(myCallable2);
//        System.out.println("提交任务之后 " + getStringDate());
//        System.out.println("开始获取第一个返回值 " + getStringDate());
//        System.out.println("获取返回值: " + future.get());
//        System.out.println("获取第一个返回值结束，开始获取第二个返回值 " + getStringDate());
//        System.out.println("获取返回值2: " + future2.get());
//        System.out.println("获取第二个返回值结束 " + getStringDate());
        ThreadPoolTest test = new ThreadPoolTest();

        System.out.println("执行之前"+getStringDate());

//        if (test.checkCustomerPrice("order")){
//            if (test.checkInventory("order")){
//
//            }else {
//
//            }
//        }else {
//
//        }

        test.processOrder("order");

        System.out.println("执行之后"+getStringDate());

    }

    public static String getStringDate() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    public boolean processOrder(String order) throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Callable<Boolean> checkPriceCallable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return checkCustomerPrice(order);
            }
        };
        Callable chechkInventoryCallable = new Callable() {
            @Override
            public Boolean call() throws Exception {
                return checkInventory(order);
            }
        };
        Future future = executor.submit(checkPriceCallable);
        Future future2 = executor.submit(chechkInventoryCallable);
        if ((boolean)future.get()){
            if ((boolean)future2.get()){
                return true;
            }else {
                return false;
            }
        }else {
            return false;
        }
    }
    public boolean checkCustomerPrice(String order) throws InterruptedException {
        Thread.sleep(5000);
        return true;
    }
    public boolean checkInventory(String order) throws InterruptedException {
        Thread.sleep(3000);
        return true;
    }
}
