package com.example.gdt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StopWatch;

import java.io.IOException;

public class gdtTestHbase {
    //创建连接
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static String startTime = "20200913";
    public static String endTime = "20200913";
    public static void main(String[] args) throws IOException {
        HBaseAdmin Admin = new HBaseAdmin(conf);
        final String thetableName=
        //广点通HBase表名（共计53张表）
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-账户管理★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取广告主信息
//         "tenant_gdt_advertiser_v2";
//        //获取广告主资质
//        "tenant_gdt_qualifications_v2";
//        //获取资金账户信息
//        "tenant_gdt_funds_v2";
//        //获取资金账户信息-代理商
//        "tenant_gdt_agent_funds_v2";
//        //获取实时消耗
//        "tenant_gdt_realtime_cost_v2";
//        //获取资金账户流水
//        "tenant_gdt_fund_stream_v2";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-日报表/小时报表★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取日报表-广告主
//        "tenant_gdt_daily_report_advertiser_v3";
//        //获取日报表-广告计划
        "tenant_gdt_daily_report_campaign_v3";
          //获取日报表-广告组
//        "tenant_gdt_daily_report_adgroup_v3";
          //获取日报表-广告
//        "tenant_gdt_daily_report_ad_v3";
          //获取小时报表-广告主
//        "tenant_gdt_hourly_report_advertiser_v2";
          //获取小时报表-广告计划
//        "tenant_gdt_hourly_report_campaign_v2";
          //获取小时报表-广告组
//        "tenant_gdt_hourly_report_adgroup_v2";
          //获取小时报表-广告
//        "tenant_gdt_hourly_report_ad_v2";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-辅助工具★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取定向标签
//        "tenant_gdt_targeting_tags_v2";
//        //获取创意规格信息-代理商
//        "tenant_gdt_adcreative_template_info_v2";
//        //获取创意规格信息
//        "tenant_gdt_adcreative_template_enable_v2";
//        //创建异步任务
//        "tenant_gdt_async_template_report";
//        //获取异步任务
//        "	tenant_gdt_async_template_report";
//        //获取文件接口
//        "tenant_gdt_async_template_report";
//        //获取推广计划
//        "tenant_gdt_campaigns_v2";
//        //获取推广计划-已删除
//        "tenant_gdt_campaigns_v2";
//        //获取广告组
//        "tenant_gdt_adgroups_v2";
//        //获取广告组-已删除
//        "tenant_gdt_adgroups_v2";
//        //获取广告创意
//        "tenant_gdt_adcreatives_v2";
//        //获取广告创意-已删除
//        "	tenant_gdt_adcreatives_v2";
//        //获取动态广告创意列表
//        "tenant_gdt_dynamic_creatives_v2";
//        //获取广告
//        "tenant_gdt_ads_v2";
//        //获取广告-已删除
//        "	tenant_gdt_ads_v2";
//        //获取定向
//        "	tenant_gdt_targetings_v2";
//        //获取推广目标
//        "tenant_gdt_promoted_objects_v2";
//        //获取落地页列表
//        "tenant_gdt_pages_v2";
//        //获取视频文件
//        "tenant_gdt_videos_v2";
//        //获取图片信息
//        "	tenant_gdt_images_v2";
//        //获取联盟流量包
//        "	tenant_gdt_union_position_packages";
//        //获取用户行为数据源
//        "tenant_gdt_user_action_sets_v2";
//        //获取客户人群
//        "tenant_gdt_custom_audiences_v2";
//        //获取客户人群-代理商
//        "tenant_gdt_custom_audiences_v2";
//        //获取客户人群数据文件
//        "tenant_gdt_custom_audience_files_v2";
//        //获取客户人群数据文件-代理商
//        "tenant_gdt_custom_audience_files_v2";
//        //人群洞察分析
//        "tenant_gdt_custom_audience_insights_v2";
//        //获取定向标签报表-广告主-年龄
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告主-性别
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告主-区域
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告主-城市
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-年龄
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-性别
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-区域
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-城市
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-年龄
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-性别
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-区域
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-城市
//        "tenant_gdt_targeting_tag_reports_v2";
        boolean tenant_jrtt_ad_get_v2 = Admin.tableExists(thetableName);
        TableName tableName = TableName.valueOf(thetableName);
        System.out.println(tableName);
        new HTable(conf,thetableName);
        HTableDescriptor tableDescriptor = Admin.getTableDescriptor(tableName);
        String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        if (! tableDescriptor.hasCoprocessor(coprocessorClass)) {
            tableDescriptor.addCoprocessor(coprocessorClass);
        }
        Admin.modifyTable(tableName, tableDescriptor);
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Scan scan = new Scan();
        LongColumnInterpreter longColumnInterpreter = new LongColumnInterpreter();
        AggregationClient aggregationClient = new AggregationClient(conf);
        int sum =0;
        for (int i=0;i <= 9;i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            try {
                long l = aggregationClient.rowCount(tableName, longColumnInterpreter, scan);
                System.out.println(l);
                sum = (int) (sum + l);
                System.out.println("总数据条数RowCount: " + sum);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        stopWatch.stop();
        System.out.println("统计耗时："+(stopWatch.getTotalTimeMillis())/1000+"秒");
    }
    public int countHbase() throws IOException {
        HBaseAdmin Admin = new HBaseAdmin(conf);
        boolean tenant_jrtt_ad_get_v2 = Admin.tableExists("tenant_jrtt_advertiser_info_v2");
        TableName tableName = TableName.valueOf("tenant_jrtt_advertiser_info_v2");
        System.out.println(tableName);
        new HTable(conf,"tenant_jrtt_advertiser_info_v2");
        HTableDescriptor tableDescriptor = Admin.getTableDescriptor(tableName);
        String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        if (! tableDescriptor.hasCoprocessor(coprocessorClass)) {
            tableDescriptor.addCoprocessor(coprocessorClass);
        }
        Admin.modifyTable(tableName, tableDescriptor);
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Scan scan = new Scan();
        LongColumnInterpreter longColumnInterpreter = new LongColumnInterpreter();
        AggregationClient aggregationClient = new AggregationClient(conf);
        int sum =0;
        for (int i=0;i <= 9;i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + "19000724"+ "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + "20200724"+ "|z"));
//            scan.setStartRow(Bytes.toBytes(i + "|" + "19900" + "|0"));
//            scan.setStopRow(Bytes.toBytes(i + "|" + "2030z" + "|z"));
            try {
                long l = aggregationClient.rowCount(tableName, longColumnInterpreter, scan);

                System.out.println(l);
                sum = (int) (sum + l);
                System.out.println("总数据条数RowCount: " + sum);
             } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
        }
//        scan.setStartRow(Bytes.toBytes("0" + "|" + "20200721" + "0"));
//        scan.setStopRow(Bytes.toBytes("0" + "|" + "20200722" + "0"));
//        AggregationClient aggregationClient = new AggregationClient(conf);
//        try {
//            System.out.println("RowCount: " + aggregationClient.rowCount(tableName,new LongColumnInterpreter(),scan));
//        } catch (Throwable throwable) {
//            throwable.printStackTrace();
//        }
        stopWatch.stop();
        System.out.println("统计耗时："+(stopWatch.getTotalTimeMillis())/1000+"秒");
        return sum;
    }
}
