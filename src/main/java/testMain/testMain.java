package testMain;

/**
 * # @Time : 2020/9/17 17:47
 * # @Author : aDiao
 * # @File : testMain.java
 * # @Software: IntelliJ IDEA
 * # @description：多线程测试
 * # @version:1.0.0.0 版本
 */
public class testMain {
    public static void main(String[] args) throws InterruptedException {
       test a = new test();
//       long b = a.add10K();
        Thread thread1 = new Thread(() -> a.add10K());
        Thread thread2 = new Thread(() -> a.add10K());
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
//        System.out.println(b);
    }
}
