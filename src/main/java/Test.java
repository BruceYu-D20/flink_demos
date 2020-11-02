package yux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class Test {

    public static void main(String[] args) throws InterruptedException {


//        long start = System.currentTimeMillis();
//        Thread baozi = new baozi();
//        baozi.start();
//        baozi.join();
//
//        Thread liangcai = new liangcai();
//        liangcai.start();
//        liangcai.join();
//
//        long end = System.currentTimeMillis();
//        System.out.println("all time : " + (end - start));

        // --------------------------------
        long futureStart = System.currentTimeMillis();
        Callable baoziFu = new Callable() {
            @Override
            public String call() throws Exception {
                Thread.sleep(3000);
                return "包子好了";
            }
        };
        FutureTask baozitask = new FutureTask(baoziFu);
        new Thread(baozitask).start();

        Callable liangcaiFu = new Callable() {
            @Override
            public Object call() throws Exception {
                Thread.sleep(1000);
                return "凉菜好了";
            }
        };
        FutureTask liangcaitask = new FutureTask(liangcaiFu);
        new Thread(liangcaitask).start();
        long futureEnd = System.currentTimeMillis();
        System.out.println("future all time : " + (futureEnd - futureStart));
    }

    public static class baozi extends Thread{
        @Override
        public void run() {
            try {
                Thread.sleep(3000);
                System.out.println("包子出来了");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class liangcai extends Thread{
        @Override
        public void run() {
            super.run();
            try {
                Thread.sleep(1000);
                System.out.println("凉菜出来了");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
