//package testThread;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//public class threadQueryHbase {
//    //创建连接
//    public static Configuration conf;
//    static {
//        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","10.0.4.45");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
//    }
//    public static void main(String[] args) throws IOException {
//        long st1 = System.currentTimeMillis();
//        HTable hTable = null;
//        HTable hTable1 = new HTable(conf, "tenant_gdt_advertiser_v2");
//        //设置scan扫表缓存数据
//        hTable1.setScannerCaching(50000);
//        ExecutorService pool = Executors.newFixedThreadPool(20);
//        final int maxRowSize = 2966941;
//
//
//    }
//
//
//
//}
