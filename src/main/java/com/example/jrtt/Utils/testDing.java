package com.example.jrtt.Utils;

/**
 * # @Time : 2020/9/10 15:51
 * # @Author : aDiao
 * # @File : testDing.java
 * # @Software: IntelliJ IDEA
 * # @description：测试钉钉推送
 * # @version:1 版本
 */
public class testDing {
    public static void main(String[] args) {
        DingRobotMsg dingRobotMsg = new DingRobotMsg();
        String msg ="今日头条全量数据推送完成";
        dingRobotMsg.dingRequest(msg);
    }
}
