package com.example.jrtt;

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

public class testHbase {
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
        //今⽇头条HBase表名（共计55张表）
                "tenant_gdt_daily_report_campaign_v3";
        //测试表的位置  parametric_test:  jrtt_child_agent_advertiser_select
//              "tenant_jrtt_advertiser_info_v2";//广告主信息
//              "tenant_jrtt_child_advertiser_info_v2";//广告主信息二代账户
//              "tenant_jrtt_qualification_get_v2";//获取广告主资质
//              "tenant_jrtt_advertiser_select_v2";//广告主列表
//              "tenant_jrtt_child_agent_select_v2";//二级代理商列表
//              "tenant_jrtt_advertiser_fund_get_v2";//查询账号余额
//              "tenant_jrtt_one_agent_fund_v2";//查询账户余额-一级代理商
//              "tenant_jrtt_agent_fund_v2";//查询账号余额-二代代理商
//              "tenant_jrtt_advertiser_fund_daily_stat_v2";//查询账号日流水
//              "tenant_jrtt_adv_fund_detail_v2";//查询账号流水明细
        //第二阶段
//              "tenant_jrtt_advertiser_budget_get";//获取账户日预算
//              "tenant_jrtt_campaign_get_v2";//获取广告组
//              "tenant_jrtt_ad_get_v2";//获取广告计划
//              "tenant_jrtt_creative_get_v2";//获取创意列表
//              "tenant_jrtt_creative_read_v2";//创意详细信息
//              "tenant_jrtt_report_advertiser_get_daily_v2";//广告主数据
//              "tenant_jrtt_report_campaign_get_daily_v2";//广告组数据
//              "tenant_jrtt_report_ad_get_daily_v2";//广告计划数据
//              "tenant_jrtt_report_creative_get_daily_v2";//广告创意数据
//              "tenant_jrtt_report_agent_get_daily_v2";//新版代理商数据
//              "tenant_jrtt_report_integrated_get_daily";//多合一数据报表接口
//              "tenant_jrtt_dmp_data_source_read_v2";//数据源详细信息
//              "tenant_jrtt_dmp_custom_audience_select_v2";//人群包列表
//              "tenant_jrtt_dmp_custom_audience_read_v2";//人群包详细信息
//              "tenant_jrtt_tools_site_v2";//获取站点列表-（获取橙子建站站点列表）
//              "parametric_test:tenant_jrtt_tools_image_v2";//获取图片素材库
//              "parametric_test:tenant_jrtt_tools_video_v2";//获取视频素材库
//              "jrtt_tools_video_cover_suggest_v2";//获取视频智能页面
//                "parametric_test:tenant_jrtt_tools_creative_word_v2";//查询动态创意词包
//              "parametric_test:tenant_jrtt_tools_industry_v2";//获取行业列表
//              "tenant_jrtt_tools_region_v2";//获取地域列表
//              "tenant_jrtt_tools_playable_list_v2";//获取试玩素材列表
//              "tenant_jrtt_tools_advertiser_package_v2";//查询极速下载列表
//              "tenant_jrtt_tools_action_category_v2";//行为类目查询
//              "parametric_test:tenant_gdt_custom_audience_files_v2";//兴趣类目查询
//              "tenant_jrtt_tools_union_flow_package_v2";//获取穿山甲流量包
//              "parametric_test:tenant_jrtt_tools_adv_convert_v2";//转化ID列表
//              "parametric_test:tenant_jrtt_tools_ad_convert_read_v2";//查询转化详细信息
//              "parametric_test:tenant_jrtt_tools_interest_tags_v2";//获取兴趣关键词词包
//              "tenant_jrtt_audience_package_v2";//获取定向包
//              "parametric_test:tenant_jrtt_report_advertiser_audience_province_v2";//受众分析数据-省级数据-广告主
//              "parametric_test:tenant_jrtt_report_campaign_audience_province_v2";//受众分析数据-省级数据-广告组
//              "parametric_test:tenant_jrtt_report_ad_audience_province_v2";//受众分析数据-省级数据-广告计划
//              "parametric_test:tenant_jrtt_report_advertiser_audience_gender_v2";//受众分析数据-性别数据-广告主
//              "parametric_test:tenant_jrtt_report_campaign_audience_gender_v2";//受众分析数据-性别数据-广告组
//              "parametric_test:tenant_jrtt_report_ad_audience_gender_v2";//受众分析数据-性别数据-广告计划
//              "parametric_test:tenant_jrtt_report_advertiser_audience_tag_v2";//受众分析数据-兴趣数据-广告主
//              "tenant_jrtt_report_campaign_audience_tag_v2";//受众分析数据-兴趣数据-广告组
//              "tenant_jrtt_report_ad_audience_tag_v2";//受众分析数据-兴趣数据-广告计划
//              "tenant_jrtt_report_advertiser_audience_age_v2";//受众分析数据-年龄数据-广告主
//              "tenant_jrtt_report_campaign_audience_age_v2";//受众分析数据-年龄数据-广告组
//              "tenant_jrtt_report_ad_audience_age_v2";//受众分析数据-年龄数据-广告计划
//              "tenant_jrtt_report_advertiser_audience_city_v2";//受众分析数据-市级数据-广告主
//              "tenant_jrtt_report_campaign_audience_city_v2";//受众分析数据-市级数据-广告组
//              "tenant_jrtt_report_ad_audience_city_v2";//受众分析数据-市级数据-广告组
        //广点通HBase表名（共计53张表）
//              "tenant_gdt_advertiser_v2";//获取广告主信息
//              "tenant_jrtt_child_advertiser_info_v2";//广告主信息
//              "tenant_gdt_qualifications_v2";//获取广告主资质
//              "tenant_gdt_funds_v2";//获取资金账户信息
//              "tenant_gdt_agent_funds_v2";//获取资金账户信息-代理商
//              "tenant_gdt_realtime_cost_v2";//获取实时消耗
//              "tenant_gdt_fund_stream_v2";//获取资金账户流水
//              "tenant_gdt_daily_report_advertiser_v3";//获取日报表-广告主
//              "tenant_gdt_daily_report_campaign_v3";//获取日报表-广告计划
//              "tenant_gdt_daily_report_adgroup_v3";//获取日报表-广告组
//              "tenant_gdt_daily_report_ad_v3";//获取日报表-广告
//              "tenant_gdt_hourly_report_advertiser_v2";//获取小时报表-广告主
//              "tenant_gdt_hourly_report_campaign_v2";//获取小时报表-广告计划
//              "tenant_gdt_hourly_report_adgroup_v2";//获取小时报表-广告族
//              "tenant_gdt_hourly_report_ad_v2";//获取小时报表-广告
//              "tenant_gdt_targeting_tags_v2";//获取定向标签
//              "tenant_gdt_adcreative_template_info_v2";//获取创意规格信息-代理商
//              "tenant_gdt_adcreative_template_enable_v2";//获取创意规格信息
//              "tenant_gdt_async_template_report";//创建异步任务
//              "tenant_gdt_async_template_report";//获取异步任务
//              "tenant_gdt_async_template_report";//广告主信息
//              "tenant_gdt_async_template_report";//获取文件接口
//              "tenant_gdt_campaigns_v2";//获取推广计划
//              "tenant_gdt_campaigns_v2";//获取推广计划-已删除
//              "tenant_gdt_adgroups_v2";//获取广告组
//              "tenant_gdt_adgroups_v2";//获取广告组-已删除
//              "tenant_gdt_adcreatives_v2";//获取广告创意
//              "tenant_gdt_adcreatives_v2";//获取广告创意-已删除
//              "tenant_gdt_dynamic_creatives_v2";//获取动态广告创意列表
//              "tenant_gdt_ads_v2";//获取广告
//              "tenant_gdt_ads_v2";//获取广告-已删除
//              "tenant_gdt_targetings_v2";//获取定向
//              "tenant_gdt_promoted_objects_v2";//获取推广目标
//              "tenant_gdt_pages_v2";//获取落地页列表
//              "tenant_gdt_videos_v2";//获取视频文件
//              "tenant_gdt_images_v2";//获取图片信息
//              "tenant_gdt_union_position_packages";//获取联盟流量包
//              "tenant_gdt_user_action_sets_v2";//获取用户行为数据源
//              "tenant_gdt_custom_audiences_v2";//获取客户人群
//              "tenant_gdt_custom_audiences_v2";//获取客户人群-代理商
//              "tenant_gdt_custom_audience_files_v2";//获取客户人群数据文件
//              "tenant_gdt_custom_audience_files_v2";//获取客户人群数据文件-代理商
//              "tenant_gdt_custom_audience_insights_v2";//人群洞察分析
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告主-年龄
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告主-性别
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告主-区域
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告主-城市
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告计划-年龄
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告计划-性别
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告计划-区域
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告计划-城市
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告组-年龄
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告组-性别
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告组-区域
//              "tenant_gdt_targeting_tag_reports_v2";//获取定向标签报表-广告组-城市
        //微信HBase表名（共计8张表）
//              "tenant_wx_advertiser";//获取微信广告主信息（待废弃）
//              "tenant_wx_advertiser_spec";//获取微信广告主信息
//              "tenant_wx_funds";//获取资金账户信息
//              "tenant_wx_agent_funds";//获取资金账户信息-代理商
//              "tenant_wx_daily_cost";//获取微信账户实时消耗
//              "tenant_wx_async_adgroup_daily_report";//创建异步任务，获取异步任务，获取文件接口
//              "tenant_wx_async_adgroup_hourly_report";//创建异步任务，获取异步任务，获取文件接口
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
