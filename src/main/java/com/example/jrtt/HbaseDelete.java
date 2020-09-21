package com.example.jrtt;

import Config.HbaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Scanner;

public class HbaseDelete {
    //创建连接
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void main(String[] args) {
        Scanner cin = new Scanner(System.in);
        String next = null;
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            System.out.println("开始清空目标表");
            //手动输入是否执行

            String tableName = HbaseConfig.tenant_jrtt_adv_fund_detail_v2;
            if (tableName.indexOf("parametric_test:") != -1) {
                System.err.println("是否执行清空表(  " + tableName + "  )的操作    yes/no");
                next = cin.next().toString();
                if (next.equals("yes") || next.equals("y")) {
                    //设置表为无状态

                    admin.disableTable(tableName);
                    //清空指定表的的数据
                    admin.truncateTable(TableName.valueOf(tableName), true);
                    System.out.println("清空表执行成功");
                }
            } else {
                System.out.println("数据库名称存在异常");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //调用方法
    public  void DeleteData(String tablename) {
        Scanner cin = new Scanner(System.in);
        String next = null;
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            System.out.println("开始清空目标表");
            //手动输入是否执行

 //           String tableName = HbaseConfig.tenant_jrtt_adv_fund_detail_v2;
            String tableName = tablename;
            if (tableName.indexOf("parametric_test:") != -1) {
                System.err.println("是否执行清空表(  " + tableName + "  )的操作    yes/no");
                next = cin.next().toString();
                if (next.equals("yes") || next.equals("y")) {
                    //设置表为无状态
                    admin.disableTable(tableName);
                    //清空指定表的的数据
                    admin.truncateTable(TableName.valueOf(tableName), true);
                    System.out.println("清空表执行成功");
                }
            } else {
                System.out.println("数据库名称存在异常");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
