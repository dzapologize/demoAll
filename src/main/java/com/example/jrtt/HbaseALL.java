package com.example.jrtt;


import com.example.jrtt.Util.deleteTxtFile;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class HbaseALL {
    //创建连接
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static String startTime = "20200913";
    public static String endTime = "20200913";
    public static String equalsTime = "2020-09-03";
    public static void main(String[] args) throws Throwable {
        HBaseAdmin Admin = new HBaseAdmin(conf);
        //parametric_test:
        final String thetableName= "parametric_test:tenant_gdt_adcreative_template_enable_v2";
//        final String thetableName= "parametric_test:tenant_jrtt_dmp_custom_audience_select_v2";
        HTable htable = new HTable(conf, thetableName);
        //得到用于扫描的region对象
        Scan scan = new Scan();
        TableName tableName = TableName.valueOf(thetableName);
        System.out.println(tableName);
        HTableDescriptor tableDescriptor = Admin.getTableDescriptor(tableName);
        String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        if (! tableDescriptor.hasCoprocessor(coprocessorClass)) {
            tableDescriptor.addCoprocessor(coprocessorClass);
        }
        Admin.modifyTable(tableName, tableDescriptor);
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        AggregationClient aggregationClient = new AggregationClient(conf);
        LongColumnInterpreter longColumnInterpreter = new LongColumnInterpreter();
        int sum =0;
        int all =0;
        for (int i=0;i <= 9;i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" +  endTime + "|z"));
            try {
                long l = aggregationClient.rowCount(tableName, longColumnInterpreter, scan);
                //System.out.println(l);
                sum = (int) (sum + l);
//                System.out.println("总数据条数RowCount: " + sum);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
            sum +=sum;
//            scan.setStartRow(Bytes.toBytes(i + "|" + "19900" + "|0"));
//            scan.setStopRow(Bytes.toBytes(i + "|" + "2030z" + "|z"));
            //使用HTable得到resultcanner实现类的对象
            ResultScanner resultScanner = htable.getScanner(scan);
            HashMap<String, HashMap<String, String>> otherRowkeyValue = new HashMap<>();
            for (Result result: resultScanner){
                Cell[] cells = result.rawCells();
                for (Cell cell : cells){
                        //得到 rowkey
//                        String allCell = Bytes.toString(CellUtil.cloneValue(cell));
//                    byte[] value = CellUtil.cloneValue(cell);
//                    System.out.println(value);
//                    if (allCell != null & allCell != "") {
//                        JSONObject jsonObject = JSONObject.fromObject(allCell);
//                        System.out.println(jsonObject);
//                        JSONObject data = (JSONObject) jsonObject.get("data");
//                        Object create_time = data.get("create_time");
////                        Object timeStamp = jsonObject.get("timestamp");
//                        String create_times = create_time.toString();
//                        System.out.println(create_time);
//                        if (create_times != null && create_times.length() != 0 & create_times != "" & create_times != "null"){
//                            String theCreate_times = create_times.substring(0, 10);
//                            if(theCreate_times.equals(equalsTime)){
//                                System.out.println("create_time         :"+theCreate_times);
//                                all = all+1;
//                            }else {
////                            System.out.println("日期异常");
//                            }
//                        }else {
//                            System.out.println("异常的rowkey"+create_time);
//                        }
//
////                        System.out.println("tenantAlias        "+tenantAlias);
////                        System.out.println("timeStamp          "+timeStamp);
//                    }
//                        System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
                         all = all+1;
                        System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
//                        //得到列族
//                        System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
//                        //得到列
//                        System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//                        //得到value
//                        System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
//                        System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
                    }
                }
            }
        //把不同的data数据存到txt里
//        deleteTxtFile deleteTxtFile = new deleteTxtFile();
//        try {
//            File file=new File("D:\\txt\\");  //  /传参是路径名
//            //删除txt文件
//            deleteTxtFile.delete(file);
//            String line = System.getProperty("line.separator");
//            StringBuffer str = new StringBuffer();
//            //创建txt文件
//            FileWriter fw = new FileWriter("D:\\txt\\otherValue.txt", true);
//            Set<Map.Entry<String, HashMap<String, String>>> set = otherRowkeyValue.entrySet();
//            Iterator<Map.Entry<String, HashMap<String, String>>> iterator = set.iterator();
//            fw.flush();
//            while (iterator.hasNext()) {
//                Map.Entry<String, HashMap<String, String>> next = iterator.next();
//                HashMap<String, String> value = next.getValue();
////                System.out.println("valueString"+value);
//                str.append(next.getKey() + "   :   " + next.getValue()).append(line);
//            }
//            fw.write(str.toString());
//            fw.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
       stopWatch.stop();
        System.out.println("统计耗时："+stopWatch.getTotalTimeMillis()/1000+"秒" + "统计数据条数     all=：" + all);
        System.out.println(endTime+"的数据条数为 sum=：" + sum);
    }
}
