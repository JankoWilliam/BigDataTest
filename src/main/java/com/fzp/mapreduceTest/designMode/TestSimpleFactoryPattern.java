package com.fzp.mapreduceTest.designMode;

public class TestSimpleFactoryPattern {
    /**
     * 工厂模式：利用工厂模式调用两个类
     */
    public static void main(String[] args) {

       CarFactory factory = new CarFactory();//创建工厂模式的 CarFactory类的对象

       Car c = factory.createCar("auti");//调用 CarFactory类中的方法创建对象

       c.run();//调用所需类中的方法

    }
}

class CarFactory{//建立工厂模式的 CarFactory类
    public Car createCar(String type){//工厂模式的 CarFactory类中的createCar方法
       if("auti".equalsIgnoreCase(type)){
           return new Audi();
       }else if("auto".equalsIgnoreCase(type)){
           return new Auto();
       }else{
           return null;
       }
    }
}

interface Car{
    public void run();
}

class Audi implements Car {
    @Override
    public void run() {
       System.out.println("奥迪车跑跑跑!");
    }

}
class Auto implements Car{
    @Override
    public void run() {
       System.out.println("奥拓车跑跑跑!");
    }

}