package com.example.jrtt;

import net.sf.json.JSONArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class queryHbaseMySQL {
    //先把rowkey查询出来存到list
    //创建连接
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }

    public static void main(String[] args) throws IOException, SQLException {
        //创建存rowkey的list
        List<String> RowkeyList = new ArrayList<String>();
        //先查表tenant_jrtt_advertiser_info_v2
        HBaseAdmin Admin = new HBaseAdmin(conf);
        boolean tableExists = Admin.tableExists("tenant_jrtt_advertiser_info_v2");
        System.out.println(tableExists);
        TableName tableName = TableName.valueOf("tenant_jrtt_advertiser_info_v2");
        HTable hTable = new HTable(conf, "tenant_jrtt_advertiser_info_v2");
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Scan scan = new Scan();
        for (int i=0;i <= 9;i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + "20200723" + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + "20200723" + "|z"));
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
                    String substring = s.substring(10, 34);
                    //System.out.println("这是rowKey"+substring);
                    RowkeyList.add(substring);
                }
            }
        }
        System.out.println("***************第一张表rowkey的总量*************"+RowkeyList.size());
        //再查表  tenant_jrtt_child_advertiser_info_v2  然后合计到rowkeyList中
        HBaseAdmin Admin2 = new HBaseAdmin(conf);
        boolean tableExists2 = Admin2.tableExists("tenant_jrtt_child_advertiser_info_v2");
        System.out.println(tableExists2);
        TableName tableName2 = TableName.valueOf("tenant_jrtt_child_advertiser_info_v2");
        HTable hTable2 = new HTable(conf, "tenant_jrtt_child_advertiser_info_v2");
        Scan scan2 = new Scan();
        for (int i=0;i <= 9;i++){
            scan2.setStartRow(Bytes.toBytes(i + "|" + "20200723" + "|0"));
            scan2.setStopRow(Bytes.toBytes(i + "|" + "20200723" + "|z"));
            ResultScanner resultScanner = hTable2.getScanner(scan2);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
                    String substring = s.substring(10, 34);
                    //System.out.println("这是rowKey"+substring);
                    RowkeyList.add(substring);
                }
            }
        }
        System.out.println("***************两张表rowkey的总量*************"+RowkeyList.size());
        //拿着rowkey的list去查mysql
        String driverClassName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.0.0.19:3306/nyfz";
        String mysqlusername = "root";
        String password = "rzvNCHj9s4wVYA";
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        // json数组
        JSONArray array = new JSONArray();
        StringBuffer stringBuffer = new StringBuffer();
        List<String> idList = new ArrayList<String>();
        int sum =0;
        try {
            Class.forName(driverClassName);
            Connection connection = DriverManager.getConnection(url, mysqlusername, password);
            System.out.println("这是connection"+connection);
            String sql ="select id,account_id,agent_id,data_hour from jrtt_fund_log_20200723 where data_date='2020-07-23' and data_hour >='12' and data_hour<='13'";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            System.out.println("这是列数"+columnCount);
            while (resultSet.next()){
                //stringBuffer.append(resultSet.getString(1));
                stringBuffer.append("|");
                stringBuffer.append(resultSet.getString(2));
                stringBuffer.append("|");
                stringBuffer.append(resultSet.getString(3));
                stringBuffer.append("|");
                //stringBuffer.append(resultSet.getString(4));
                String s2 = stringBuffer.toString();
                idList.add(s2);
                stringBuffer.delete(0,stringBuffer.length());
                sum =sum +1;
                //System.out.println("这是MySQL的account_ID:"+s2);
            }
            System.out.println(idList.size());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        List<String> otherList = new ArrayList<String>();
        for (int i =0;i <RowkeyList.size();i++ ){
            String s = RowkeyList.get(i);
            boolean b = idList.contains(s);
            if (b = false){
                otherList.add(s);
            }
        }
        for (int i=0;i < otherList.size();i ++){
            System.out.println("这是异常值"+otherList.get(i));
        }
        System.out.println("RowkeyList"+RowkeyList.size());
        System.out.println("idList"+idList.size());
    }


}
