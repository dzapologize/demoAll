package testMain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.util.StopWatch;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class testSpark {
    //创建连接
    public static Configuration conf;
    public static String tableName = "tenant_gdt_advertiser_v2";
    static {
        conf = HBaseConfiguration.create();
        conf.set("spark.serializer","org.apache .bark.serializer.KryoSerializer");
        conf.set("hbase.zookeeper.quorum","10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    //读取数据速度
    public static void main(String[] args) throws IOException {
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder()
                .appName("lcc_java_read_hbase_register_to_table")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
        Scan scan = new Scan();
        scan.setCaching(100000);
        scan.setCacheBlocks(false);
        conf.set(TableInputFormat.INPUT_TABLE,tableName);
        int sum = 0;
        for (int i = 0;i <= 9; i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + "19000000" + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + "20200727" + "|z"));
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN,scanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = context.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            long count = myRDD.count();
            System.out.println(count);
            myRDD.keys();
            sum = (int) (sum + count);
            System.out.println("第"+i+"次循环时sum合计数据条数"+sum);
        }
        stopWatch.stop();
        System.out.println("统计耗时："+(stopWatch.getTotalTimeMillis())/1000+"秒"+"总计数据：  " + sum);
        //        List<Tuple2<ImmutableBytesWritable, Result>> list = myRDD.collect();
//        for (int i = 0; i < list.size();i++){
//            System.out.println(list.get(i));
//        }
    }

    //spark读取查询数据
    public void sparkQuery() throws IOException {
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkSession spark = SparkSession.builder()
                .appName("lcc_java_read_hbase_register_to_table")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

        Scan scan = new Scan();
        String tableName = "tenant_gdt_advertiser_v2";
        conf.set(TableInputFormat.INPUT_TABLE,tableName);
        int sum = 0;
        for (int i = 0;i <= 9; i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + "19000000" + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + "20200727" + "|z"));
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN,scanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = context.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            long count = myRDD.count();
            System.out.println(count);
            List<Tuple2<ImmutableBytesWritable, Result>> collect = myRDD.collect();
            System.out.println(collect);
            collect.clear();
            sum = (int) (sum + count);
        }
        stopWatch.stop();
        System.out.println("统计耗时："+(stopWatch.getTotalTimeMillis())/1000+"秒"+"总计数据：  " + sum);
    }
}

