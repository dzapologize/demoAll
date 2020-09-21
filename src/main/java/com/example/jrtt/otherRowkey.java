package com.example.jrtt;

import com.example.jrtt.Utils.DingRobotMsg;
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

public class otherRowkey {
    //创建连接
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    public static String startTime = "20200914";
    public static String endTime = "20200914";
    public static String tableName =
//            "tenant_gdt_daily_report_advertiser_v3";
            //第一阶段/账号服务1/先执行广告主列表lzy       执行命令//    bash ./jrtt.sh tools/ToolsInterestCategory
//            "tenant_jrtt_advertiser_info_v2";//广告主信息gjx   / advertiser/AdvertiserInfo
//              "tenant_jrtt_child_advertiser_info_v2";//广告主信息二代账户gjx/  /   advertiser/ChildAdvertiserInfo
//				"tenant_jrtt_qualification_get_v2";//获取广告主资质gjx    /    advertiser/AdvertiserQualification
//				"tenant_jrtt_advertiser_select_v2";//广告主列表lzy       /   advertiser/AgentAdv
//				"tenant_jrtt_child_agent_select_v2";//二级代理商列表lzy      / advertiser/ChildAgentAdv
//				"tenant_jrtt_advertiser_fund_daily_stat_v2";//查询账号日流水lzt   /    advertiser/FundDailyStat
//				"tenant_jrtt_adv_fund_detail_v2";//查询账号流水明细lzy      /   advertiser/AdvFundTrans
    //第一阶段/⼴告管理2
//				"tenant_jrtt_advertiser_budget_get";//获取账户日预算lzt  / ad/AdvertiserBudget
//              "tenant_jrtt_campaign_get_v2";//获取广告组jhl    /   ad/Campaign
//              "tenant_jrtt_ad_get_v2";//获取广告计划lzt        /为了提升数据准确性，补当天的数据/会影响到创意详情和受众分析数据    ad/AdGet
//              "tenant_jrtt_creative_get_v2";//获取创意列表/gjx  /   ad/Creative
//              "tenant_jrtt_creative_read_v2";//创意详细信息/gjx/需要先跑（获取创意列表）     /   ad/CreativeRead
    //第一阶段/数据报表一期3
//				"tenant_jrtt_report_advertiser_get_daily_v2";//广告主数据表1/jhl  /   report/daily/AdvertiserDailyReport
//				"tenant_jrtt_report_advertiser_get_hourly_v2";//广告主数据表2     /   report/hourly/AdvertiserHourlyReport
//				"tenant_jrtt_report_campaign_get_daily_v2";//广告组 数据表1/jhl   /   report/daily/CampaignDailyReport
//				"tenant_jrtt_report_campaign_get_hourly_v2";//广告组 数据表2      /   report/hourly/CampaignHourlyReport
//				"tenant_jrtt_report_ad_get_daily_v2";//广告计划数据表1             /   report/daily/AdDailyReport
//				"tenant_jrtt_report_ad_get_hourly_v2";//广告计划数据表2            /   report/hourly/AdHourlyReport
//				"tenant_jrtt_report_creative_get_daily_v2";//广告创意 数据表1      /   report/daily/CreativeDailyReport
//				"tenant_jrtt_report_creative_get_hourly_v2";//广告创意 数据表2     /   report/hourly/CreativeHourlyReport
//				"tenant_jrtt_report_agent_get_daily_v2";//新版代理商数据表1/jhl     /   report/daily/AgentDailyReport
//				"tenant_jrtt_report_agent_get_hourly_v2";//新版代理商数据表2        /   report/hourly/AgentHourlyReport
//				"tenant_jrtt_report_integrated_get_daily";//多合一数据报表接口       /   report/daily/IntegratedDailyReport
    //第一阶段/人群管理4
//				"tenant_jrtt_dmp_data_source_read_v2";//数据源详细信息/lzt/   audience/DmpDataSourceRead
				"tenant_jrtt_dmp_custom_audience_select_v2";//人群包列表/gjx            /   audience/DmpCustomAudienceSelect
//				"tenant_jrtt_dmp_custom_audience_read_v2";//人群包详细信息lzy/依赖人群包列表      /   audience/DmpCusaudRead
    //第一阶段/工具管理5/依赖头任务
//				"tenant_jrtt_tools_site_v2";//获取站点列表（获取橙子建站站点列表）        /   tools/ToolSite
//				"tenant_jrtt_tools_image_v2";//获取图片素材      /    tools/ToolsImage
//				"tenant_jrtt_tools_video_v2";//获取视频素材       /   tools/ToolsVideo
//				"jrtt_tools_video_cover_suggest_v2";//获取视频智能封面      /
//				"tenant_jrtt_tools_creative_word_v2";//获取动态创意词包     /tools/CreativeWordGet
//				"tenant_jrtt_tools_industry_v2";//获取行业列表        /tools/ToolsIndustry
//				"tenant_jrtt_tools_region_v2";//获取地域列表          /tools/ToolsRegion
//				"tenant_jrtt_tools_playable_list_v2";//获取试玩素材列表     /tools/ToolsPlayableList
//				"tenant_jrtt_tools_advertiser_package_v2";//查询极速下载列表    /tools/ToolsAdvertiserPackageGet
//				"tenant_jrtt_tools_action_category_v2";//行为类目查询     /tools/ToolsActionCategory
//				"tenant_jrtt_tools_interest_category_v2";//兴趣类目查询   /tools/ToolsInterestCategory
//              "tenant_jrtt_tools_union_flow_package_v2";//获取穿山甲流量包    /tools/ToolsUnionFlowPackage
//              "tenant_jrtt_tools_adv_convert_v2";//转化ID列表             /tools/ToolsAdvConvertSelect
//              "tenant_jrtt_tools_ad_convert_read_v2";//查询转化详细信息       /tools/ToolAdConvertRead
//              "tenant_jrtt_tools_interest_tags_v2";//获取兴趣关键词词包        /tools/ToolsInterestTags
//              "tenant_jrtt_audience_package_v2";//获取定向包           /tools/ToolsAudiencePackage
    //第一阶段/数据报表二期6
//              "tenant_jrtt_report_advertiser_audience_province_v2";//受众分析数据-省级数据-广告主      /report/audience/ReportProvinceAdv
//              "tenant_jrtt_report_campaign_audience_province_v2";//受众分析数据-省级数据-广告组      /report/audience/ReportProvinceCam
//              "tenant_jrtt_report_ad_audience_province_v2";//受众分析数据-省级数据-广告计划      /report/audience/ReportProvinceAd
//              "tenant_jrtt_report_advertiser_audience_gender_v2";//受众分析数据-性别数据-广告主      /report/audience/ReportGenderAdv
//              "tenant_jrtt_report_campaign_audience_gender_v2";//受众分析数据-性别数据-广告组      /report/audience/ReportGenderCam
//              "tenant_jrtt_report_ad_audience_gender_v2";//受众分析数据-性别数据-广告计划      /report/audience/ReportGenderAd
//              "tenant_jrtt_report_advertiser_audience_tag_v2";//受众分析数据-兴趣数据-广告主      /report/audience/ReportTagAdv
//              "tenant_jrtt_report_campaign_audience_tag_v2";//受众分析数据-兴趣数据-广告组      /report/audience/
//              "tenant_jrtt_report_ad_audience_tag_v2";//受众分析数据-兴趣数据-广告计划      /report/audience/ReportTagAd
//              "tenant_jrtt_report_advertiser_audience_age_v2";//受众分析数据-年龄数据-广告主      /report/audience/ReportAgeAdv
//              "tenant_jrtt_report_campaign_audience_age_v2";//受众分析数据-年龄数据-广告组      /report/audience/ReportAgeCam
//              "tenant_jrtt_report_ad_audience_age_v2";//受众分析数据-年龄数据-广告计划      /report/audience/ReportAgeAd
//              "tenant_jrtt_report_advertiser_audience_city_v2";//受众分析数据-市级数据-广告主      /report/audience/ReportCityAdv
//              "tenant_jrtt_report_campaign_audience_city_v2";//受众分析数据-市级数据-广告组      /report/audience/ReportCityCam
//              "tenant_jrtt_report_ad_audience_city_v2";//受众分析数据-市级数据-广告计划      /report/audience/ReportCityAd

    public static String testTableName = "parametric_test:" + tableName;

    //    public static String startTime = "20200806";
//    public static String endTime = "20200806";
    public static void main(String[] args) throws IOException {
        //正式表全量rowkey
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<String> RowkeyList = new ArrayList<String>();
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
                    RowkeyList.add(s);
                }
            }
        }
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

        StringBuffer all = new StringBuffer("正式表" + tableName + "的RowkeyList的长度为： " + RowkeyList.size()+"\n");
        String str2="测试表" + testTableName + "的RowkeyList的长度为： " + RowkeyListTest.size()+"\n";
        String str3="当时间为" + startTime + "~" + endTime + "时两表差异的rowkey量为：" + RowkeyListOther.size();
//        all.append(str2);
//        all.append(str3);
//        String msg = all.toString();
//        System.out.println("=================================================================");
//        System.out.println(msg);
//        System.out.println("=================================================================");
//        DingRobotMsg dingRobotMsg = new DingRobotMsg();
//        dingRobotMsg.dingRequest(msg);
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
