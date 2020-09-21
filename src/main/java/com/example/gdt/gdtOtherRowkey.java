package com.example.gdt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class gdtOtherRowkey {
    //创建连接
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    public static String startTime = "20200916";
    public static String endTime = "20200916";
    public static String tableName =
            //广点通HBase表名（共计55张表）
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-账户管理★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取广告主信息  advertiser/AdvertiserProducer
//            "tenant_gdt_advertiser_v2";
        //获取广告主资质      advertiser/QualificationsProducer
//        "tenant_gdt_qualifications_v2";
        //获取实时消耗        advertiser/RealtimeCostProducer
//        "tenant_gdt_realtime_cost_v2";
        //获取资金账户流水    advertiser/FundStatementsDetailedProducer
//        "tenant_gdt_fund_stream_v2";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-日报表/小时报表★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        //获取日报表-广告主   report/daily/AdvertiserDailyReport
//        "tenant_gdt_daily_report_advertiser_v3";
//        //获取日报表-广告计划  report/daily/CampaignDailyReportProducer
//        "tenant_gdt_daily_report_campaign_v3";
    //获取日报表-广告组         report/daily/AdgroupDailyReportProducer
//        "tenant_gdt_daily_report_adgroup_v3";
    //获取日报表-广告           report/daily/AdDailyReportProducer
//        "tenant_gdt_daily_report_ad_v3";
    //获取小时报表-广告主        report/hourly/AdvertiserHourlyReportProducer
//        "tenant_gdt_hourly_report_advertiser_v2";
    //获取小时报表-广告计划       report/hourly/CampaignHourlyReportProducer
//        "tenant_gdt_hourly_report_campaign_v3";
    //获取小时报表-广告组         report/hourly/AdgroupHourlyReportProducer
//        "tenant_gdt_hourly_report_adgroup_v3";
    //获取小时报表-广告          report/hourly/AdHourlyReportProducer
//        "tenant_gdt_hourly_report_ad_v3";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-辅助工具★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取定向标签                                       tools/TargetingTagsProducer
//        "tenant_gdt_targeting_tags_v2";
//        //获取创意规格信息-代理商                              tools/AdcreativeTempFromAgentProducer
//        "tenant_gdt_adcreative_template_info_v2";
//        //获取创意规格信息                                    tools/AdcreativeTempFromAccountProducer
//        "tenant_gdt_adcreative_template_enable_v2";
//        ★★★//创建异步任务              tools/
//        ★★★ "tenant_gdt_async_template_report";
//        ★★★//获取异步任务              tools/
//        ★★★"	tenant_gdt_async_template_report";
//        ★★★//获取文件接口              tools/
//        ★★★ "tenant_gdt_async_template_report";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-广告管理★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取推广计划                                      ad/CampaignGetProducer
//        "tenant_gdt_campaigns_v2";
//        //获取推广计划-已删除                                 /同上
//        "tenant_gdt_campaigns_v2";
//        //获取广告组                                         ad/AdgroupGetProducer
//        "tenant_gdt_adgroups_v2";
//        //获取广告组-已删除                                   /同上
//        "tenant_gdt_adgroups_v2";
//        //获取广告创意                                       ad/CreativeGetProducer
//        "tenant_gdt_adcreatives_v2";
//        //获取广告创意-已删除                                  /同上
//        "tenant_gdt_adcreatives_v2";
//        //获取动态广告创意列表                                ad/DynamicCreativesProducer
//        "tenant_gdt_dynamic_creatives_v2";
//        //获取广告                                          ad/AdsGetProducer
//        "tenant_gdt_ads_v2";
//        //获取广告-已删除                                    /同上
//        "tenant_gdt_ads_v2";
//        //获取定向                                            ad/TargetingsGetProducer
//        "tenant_gdt_targetings_v2";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-营销资产★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取推广目标                                          assests/PromotedObjectsProducer
//        "tenant_gdt_promoted_objects_v2";
//        //获取落地页列表                                         assests/PagesProducer
//        "tenant_gdt_pages_v2";
//        //获取视频文件                                          assests/VideosProducer
//        "tenant_gdt_videos_v2";
//        //获取图片信息                                          assests/ImagesProducer
        "tenant_gdt_images_v2";
//        //获取联盟流量包                                         assests/UnionPositionPackagesProducer
//        "	tenant_gdt_union_position_packages";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-用户行为数据★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取用户行为数据源                                       custom/UserActionSetsProducer
//        "tenant_gdt_user_action_sets_v2";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-用户人群数据★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取客户人群                                              custom/GdtCustomAudiencesProducer
//        "tenant_gdt_custom_audiences_v2";
//        //获取客户人群-代理商                                       custom//GdtCustomAudiencesAgencyProducer
//          "tenant_gdt_custom_audiences_v2";
//        //获取客户人群数据文件                                       custom/CustomAudFilesProducer
//        "tenant_gdt_custom_audience_files_v2";
//        //获取客户人群数据文件-代理商                                 custom/CustomAudFilesAgentProducer
//        "tenant_gdt_custom_audience_files_v2";
//        //人群洞察分析                                             custom/CustomAudienceInsightsProducer
//        "tenant_gdt_custom_audience_insights_v2";
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★广点通接口-定向标签报表★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取定向标签报表-广告主-年龄                               report/targeting/ReportAgeAdvProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告主-性别                               report/targeting/ReportAdvertiserGenderProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告主-区域                               report/targeting/AdvertiserRegionProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告主-城市                               report/targeting/ReportAdvertiserCityProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-年龄                             report/targeting/ReportAgeCamProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-性别                             report/targeting/ReportGenderCampaignProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-区域                             report/targeting/CampaignRegionProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告计划-城市                             report/targeting/ReportCityCampaignProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-年龄                               report/targeting/ReportAgeAdProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-性别
//        report/targeting/ReportGenderAdgroupProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-区域                               report/targeting/AdGroupRegionProducer
//        "tenant_gdt_targeting_tag_reports_v2";
//        //获取定向标签报表-广告组-城市                               report/targeting/ReportCityAdgroupProducer
//        "tenant_gdt_targeting_tag_reports_v2";

    public static String testTableName = "parametric_test:" + tableName;
    public static String tag = "CITY";// AGE  年龄   GENDER  性别    REGION   区域   CITY  城市

    //    public static String startTime = "20200806";
//    public static String endTime = "20200806";
    public static void main(String[] args) throws IOException {
        //正式表全量rowkey
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<String> RowkeyLists = new ArrayList<String>();
        HBaseAdmin Admin = new HBaseAdmin(conf);
        boolean tenant_jrtt_ad_get_v2 = Admin.tableExists(tableName);
//        TableName tableName = TableName.valueOf("tenant_jrtt_advertiser_fund_get_v2");
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        for (int i = 0; i <= 9; i++) {
            scan.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
                    System.out.println(s);
                    RowkeyLists.add(s);
                    //判断受众分析标签分类
//                    if(s.contains(tag)){
//                        System.out.println(s);
//                        RowkeyLists.add(s);
//                    }
                }
            }
        }
        List<String> RowkeyList = RowkeyLists.stream().distinct().collect(Collectors.toList());
        ////测试表全量rowkey
        List<String> RowkeyListTest = new ArrayList<String>();
        TableName tableName23 = TableName.valueOf(testTableName);
        HTable hTable23 = new HTable(conf, testTableName);
        Scan scan23 = new Scan();
        for (int i = 0; i <= 9; i++) {
            scan23.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan23.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            ResultScanner resultScanner = hTable23.getScanner(scan23);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
                    System.out.println(s);
                    RowkeyListTest.add(s);
                    //判断受众分析标签分类
//                    if(s.contains(tag)) {
//                        System.out.println(s);
//                        RowkeyListTest.add(s);
//                    }
                }
            }
        }
        //找出差异的rowkey
        List<String> RowkeyListOther = new ArrayList<String>();
        Map<String, Integer> map = new HashMap<String, Integer>(RowkeyList.size() + RowkeyListTest.size());
        List<String> maxList = RowkeyList;
        List<String> minList = RowkeyListTest;
        if (RowkeyListTest.size() > RowkeyList.size()) {
            maxList = RowkeyListTest;
            minList = RowkeyList;
        }
        for (String string : maxList) {
            map.put(string, 1);
        }
        for (String string : minList) {
            Integer count = map.get(string);
            if (count != null) {
                map.put(string, ++count);
                continue;
            }
            map.put(string, 1);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() == 1) {
                RowkeyListOther.add(entry.getKey());
            }
        }
        stopWatch.stop();
        System.out.println("正式表" + tableName + "的RowkeyList的长度为： " + RowkeyList.size());
        System.out.println("测试表" + testTableName + "的RowkeyList的长度为： " + RowkeyListTest.size());
        System.out.println("当时间为" + startTime + "~" + endTime + "时两表差异的rowkey量为：" + RowkeyListOther.size());
//        for (int i =0;i < RowkeyListOther.size();i++){
//            System.out.println(RowkeyListOther.get(i));
//        }
        System.out.println("统计耗" +
                "时：" + (stopWatch.getTotalTimeMillis()) / 1000 + "秒");

    }
    //多个查询调用方法
    public List queryAll(String tablename) throws IOException {
        //正式表全量rowkey
        String testTablename = "parametric_test:" + tablename;
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<String> RowkeyList = new ArrayList<String>();
        HBaseAdmin Admin = new HBaseAdmin(conf);
        boolean tenant_jrtt_ad_get_v2 = Admin.tableExists(tablename);
//        TableName tableName = TableName.valueOf("tenant_jrtt_advertiser_fund_get_v2");
        HTable hTable = new HTable(conf, tablename);
        Scan scan = new Scan();
        for (int i = 0; i <= 9; i++) {
            scan.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
//                    System.out.println(s);
                    RowkeyList.add(s);
                }
            }
        }
        ////测试表全量rowkey
        List<String> RowkeyListTest = new ArrayList<String>();
        TableName tableName23 = TableName.valueOf(testTablename);
        HTable hTable23 = new HTable(conf, testTablename);
        Scan scan23 = new Scan();
        for (int i = 0; i <= 9; i++) {
            scan23.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan23.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            ResultScanner resultScanner = hTable23.getScanner(scan23);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
//                    System.out.println(s);
                    RowkeyListTest.add(s);
                }
            }
        }
        //找出差异的rowkey
        List<String> RowkeyListOther = new ArrayList<String>();
        Map<String, Integer> map = new HashMap<String, Integer>(RowkeyList.size() + RowkeyListTest.size());
        List<String> maxList = RowkeyList;
        List<String> minList = RowkeyListTest;
        if (RowkeyListTest.size() > RowkeyList.size()) {
            maxList = RowkeyListTest;
            minList = RowkeyList;
        }
        for (String string : maxList) {
            map.put(string, 1);
        }
        for (String string : minList) {
            Integer count = map.get(string);
            if (count != null) {
                map.put(string, ++count);
                continue;
            }
            map.put(string, 1);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() == 1) {
                RowkeyListOther.add(entry.getKey());
            }
        }
        stopWatch.stop();
        System.out.println("正式表" + tablename + "的RowkeyList的长度为： " + RowkeyList.size());
        System.out.println("测试表" + testTablename + "的RowkeyList的长度为： " + RowkeyListTest.size());
        System.out.println("当时间为" + startTime + "~" + endTime + "时两表差异的rowkey量为：" + RowkeyListOther.size());
        ArrayList<String> soutList = new ArrayList<>();
        soutList.add("正式表" + tablename + "的RowkeyList的长度为： " + RowkeyList.size()+"测试表" + testTablename + "的RowkeyList的长度为： " + RowkeyListTest.size() +"当时间为" + startTime + "~" + endTime + "时两表差异的rowkey量为：" + RowkeyListOther.size());
//        for (int i =0;i < RowkeyListOther.size();i++){
//            System.out.println(RowkeyListOther.get(i));
//        }
        System.out.println("统计耗" +
                "时：" + (stopWatch.getTotalTimeMillis()) / 1000 + "秒");
//        for (int i =0; i < RowkeyListOther.size();i++){
//            System.out.println(RowkeyListOther.get(i));jinti
//        }
        return soutList;
    }
}
