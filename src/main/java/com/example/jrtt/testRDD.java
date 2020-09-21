package com.example.jrtt;

import java.io.IOException;
import java.util.ArrayList;

public class testRDD {

    public static void main(String[] args) throws IOException {
        //遍历清空表
        HbaseDelete delete = new HbaseDelete();
        ArrayList<String> tableList = new ArrayList<>();
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★test-adv-1,2,3★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //广告主列表lzy
//        tableList.add("parametric_test:tenant_jrtt_advertiser_select_v2");
//////        //新版代理商数据表1
//        tableList.add("parametric_test:tenant_jrtt_report_agent_get_daily_v2");
////        广告主信息gjx
//        tableList.add("parametric_test:tenant_jrtt_advertiser_info_v2");
//        //获取广告主资质gjx
//        tableList.add("parametric_test:tenant_jrtt_qualification_get_v2");
//        //二级代理商列表lzy
//        tableList.add("parametric_test:tenant_jrtt_child_agent_select_v2");
//        //广告主信息二代账户gjx
//        tableList.add("parametric_test:tenant_jrtt_child_advertiser_info_v2");
//        //查询账号日流水lzt
//        tableList.add("parametric_test:tenant_jrtt_advertiser_fund_daily_stat_v2");
//        //查询账号流水明细lzy
//        tableList.add("parametric_test:tenant_jrtt_adv_fund_detail_v2");
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★test-crown-1,2,3★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //获取账户日预算lzt
//        tableList.add("parametric_test:tenant_jrtt_advertiser_budget_get");
//        //获取广告组jhl
//        tableList.add("parametric_test:tenant_jrtt_campaign_get_v2");
//        //获取广告计划jhl
//        tableList.add("parametric_test:tenant_jrtt_ad_get_v2");
        //获取创意列表
//        tableList.add("parametric_test:tenant_jrtt_creative_get_v2");
//        //广告主数据day表1/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_get_daily_v2");
//        //广告主数据hourly表2/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_get_hourly_v2");
//        //人群包列表/gjx
//        tableList.add("parametric_test:tenant_jrtt_dmp_custom_audience_select_v2");
//        //广告组day表1/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_campaign_get_daily_v2");
//        //广告组hourly表2/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_campaign_get_hourly_v2");
//        //广告计划day表1/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_ad_get_daily_v2");
//        //广告计划hourly2/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_ad_get_hourly_v2");
//        //广告创意day表1/jhl
//        tableList.add("parametric_test:tenant_jrtt_report_creative_get_daily_v2");
//        //广告创意hourly表2/jhl
//        //tableList.add("parametric_test:tenant_jrtt_report_creative_get_hourly_v2");
//        //新版代理商hourly表2
//        tableList.add("parametric_test:tenant_jrtt_report_agent_get_hourly_v2");
//        //多合一数据报表接口
//        tableList.add("parametric_test:tenant_jrtt_report_integrated_get_daily");
//        //人群包详细信息lzy
//        tableList.add("parametric_test:tenant_jrtt_dmp_custom_audience_read_v2");
//        //数据源详细信息/lzt
//        tableList.add("parametric_test:tenant_jrtt_dmp_data_source_read_v2");
//        //创意详细信息gjx
//        tableList.add("parametric_test:tenant_jrtt_creative_read_v2");
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★test-data-1,2,3★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        //                                     data1
////        受众分析数据-省级数据-广告主
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_audience_province_v2");
//        //受众分析数据-性别数据-广告主
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_audience_gender_v2");
//        //受众分析数据-兴趣数据-广告主
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_audience_tag_v2");
//        //受众分析数据-年龄数据-广告主
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_audience_age_v2");
//        //受众分析数据-市级数据-广告主
//        tableList.add("parametric_test:tenant_jrtt_report_advertiser_audience_city_v2");
////        转化ID列表
//        tableList.add("parametric_test:tenant_jrtt_tools_adv_convert_v2");
//        //                                     data2
////        获取地域列表
////        获取站点列表（获取橙子建站站点列表）
//        tableList.add("parametric_test:tenant_jrtt_tools_site_v2");
//        //获取图片素材
//        tableList.add("parametric_test:tenant_jrtt_tools_image_v2");
//        //获取视频素材
//        tableList.add("parametric_test:tenant_jrtt_tools_video_v2");
//        //获取动态创意词包
//        tableList.add("parametric_test:tenant_jrtt_tools_creative_word_v2");
//        //获取行业列表
//        tableList.add("parametric_test:tenant_jrtt_tools_industry_v2");
//        //获取地域列表
//        tableList.add("parametric_test:tenant_jrtt_tools_region_v2");
//        //获取试玩素材列表
//        tableList.add("parametric_test:tenant_jrtt_tools_playable_list_v2");
//        //查询极速下载列表
//        tableList.add("parametric_test:tenant_jrtt_tools_advertiser_package_v2");
//        //行为类目查询
//        tableList.add("parametric_test:tenant_jrtt_tools_action_category_v2");
//        //兴趣类目查询
//        tableList.add("parametric_test:tenant_jrtt_tools_interest_category_v2");
//        //获取穿山甲流量包
//        tableList.add("parametric_test:tenant_jrtt_tools_union_flow_package_v2");
//        //查询转化详细信息
        tableList.add("parametric_test:tenant_jrtt_tools_ad_convert_read_v2");
//        //获取兴趣关键词词包
//        tableList.add("parametric_test:tenant_jrtt_tools_interest_tags_v2");
//        //获取定向包
//        tableList.add("parametric_test:tenant_jrtt_audience_package_v2");
//        //                                     data3
//        //受众分析数据-省级数据-广告组
//        tableList.add("parametric_test:tenant_jrtt_report_campaign_audience_province_v2");
//        //受众分析数据-省级数据-广告计划
//        tableList.add("parametric_test:tenant_jrtt_report_ad_audience_province_v2");
//        //受众分析数据-性别数据-广告组
//        tableList.add("parametric_test:tenant_jrtt_report_campaign_audience_gender_v2");
//        //受众分析数据-性别数据-广告计划
//        tableList.add("parametric_test:tenant_jrtt_report_ad_audience_gender_v2");
//        //受众分析数据-兴趣数据-广告组
//        //tableList.add("parametric_test:tenant_jrtt_report_campaign_audience_tag_v2");
//        //受众分析数据-兴趣数据-广告计划
//        tableList.add("parametric_test:tenant_jrtt_report_ad_audience_tag_v2");
//        //受众分析数据-年龄数据-广告组
//        tableList.add("parametric_test:tenant_jrtt_report_campaign_audience_age_v2");
//        //受众分析数据-年龄数据-广告计划
//        tableList.add("parametric_test:tenant_jrtt_report_ad_audience_age_v2");
//        //受众分析数据-市级数据-广告组
//        tableList.add("parametric_test:tenant_jrtt_report_campaign_audience_city_v2");
//        //受众分析数据-市级数据-广告计划
//        tableList.add("parametric_test:tenant_jrtt_report_ad_audience_city_v2");
        //遍历删除
        for (int i = 0; i <tableList.size(); i++) {
            delete.DeleteData(tableList.get(i));
        }

    }
}
