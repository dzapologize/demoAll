package Config;

public class HbaseConfig {
    //清空表的配置
    //第一阶段/账号服务1
    public static final String tenant_jrtt_advertiser_info_v2 = "parametric_test:tenant_jrtt_advertiser_info_v2";//广告主信息gjx   advertiser/AdvertiserInfoGet
    public static final String tenant_jrtt_child_advertiser_info_v2 = "parametric_test:tenant_jrtt_child_advertiser_info_v2";//广告主信息二代账户gjx/下午跑有数据/
    public static final String tenant_jrtt_qualification_get_v2 = "parametric_test:tenant_jrtt_qualification_get_v2";//g获取广告主资质gjx/表名不一致
    public static final String tenant_jrtt_advertiser_select_v2 = "parametric_test:tenant_jrtt_advertiser_select_v2";//广告主列表lzy
    public static final String tenant_jrtt_child_agent_select_v2 = "parametric_test:tenant_jrtt_child_agent_select_v2";//二级代理商列表lzy
    public static final String tenant_jrtt_advertiser_budget_get = "parametric_test:tenant_jrtt_advertiser_budget_get";//查询账号余额lzt账户余额已上线
    public static final String tenant_jrtt_one_agent_fund_v2 = "parametric_test:tenant_jrtt_one_agent_fund_v2";//查询账户余额-一级代理商lzt
    public static final String tenant_jrtt_agent_fund_v2 = "parametric_test:tenant_jrtt_agent_fund_v2";//g查询账号余额-二代代理商lzt
    public static final String tenant_jrtt_advertiser_fund_daily_stat_v2 = "parametric_test:tenant_jrtt_advertiser_fund_daily_stat_v2";//查询账号日流水lzt advertiser/FundDailyStat
    public static final String tenant_jrtt_adv_fund_detail_v2 = "parametric_test:tenant_jrtt_adv_fund_detail_v2";//查询账号流水明细lzy
    //第一阶段/⼴告管理2
//    public static final String tenant_jrtt_advertiser_budget_get = "tenant_jrtt_advertiser_budget_get";//获取账户日预算lzt  ad/AdvertiserBudget
    public static final String tenant_jrtt_campaign_get_v2 = "parametric_test:tenant_jrtt_campaign_get_v2";//获取广告组jhl
    public static final String tenant_jrtt_ad_get_v2 = "parametric_test:tenant_jrtt_ad_get_v2";//获取广告计划jhl
    public static final String tenant_jrtt_creative_get_v2 = "parametric_test:tenant_jrtt_creative_get_v2";//获取创意列表/gjx
    public static final String tenant_jrtt_creative_read_v2 = "parametric_test:tenant_jrtt_creative_read_v2";//创意详细信息/gjx
    //第一阶段/数据报表一期3
    public static final String tenant_jrtt_report_advertiser_get_daily_v2 = "parametric_test:tenant_jrtt_report_advertiser_get_daily_v2";//广告主数据表1/jhl
    public static final String tenant_jrtt_report_advertiser_get_hourly_v2 = "parametric_test:tenant_jrtt_report_advertiser_get_hourly_v2";//广告主数据表2
    public static final String tenant_jrtt_report_campaign_get_daily_v2 = "parametric_test:tenant_jrtt_report_campaign_get_daily_v2";//广告组 数据表1/jhl
    public static final String tenant_jrtt_report_campaign_get_hourly_v2 = "parametric_test:tenant_jrtt_report_campaign_get_hourly_v2";//广告组 数据表2
    public static final String tenant_jrtt_report_ad_get_daily_v2 = "parametric_test:tenant_jrtt_report_ad_get_daily_v2";//广告计划数据表1
    public static final String tenant_jrtt_report_ad_get_hourly_v2 = "parametric_test:tenant_jrtt_report_ad_get_hourly_v2";//广告计划数据表2
    public static final String tenant_jrtt_report_creative_get_daily_v2 = "parametric_test:tenant_jrtt_report_creative_get_daily_v2";//广告创意 数据表1
    public static final String tenant_jrtt_report_creative_get_hourly_v2 = "parametric_test:tenant_jrtt_report_creative_get_hourly_v2";//广告创意 数据表2
    public static final String tenant_jrtt_report_agent_get_daily_v2 = "parametric_test:tenant_jrtt_report_agent_get_daily_v2";//新版代理商数据表1/jhl
    public static final String tenant_jrtt_report_agent_get_hourly_v2 = "parametric_test:tenant_jrtt_report_agent_get_hourly_v2";//新版代理商数据表2
    public static final String tenant_jrtt_report_integrated_get_daily = "parametric_test:tenant_jrtt_report_integrated_get_daily";//多合一数据报表接口
    //第一阶段/人群管理4
    public static final String tenant_jrtt_dmp_data_source_read_v2 = "parametric_test:tenant_jrtt_dmp_data_source_read_v2";//数据源详细信息/lzt/依赖人群包列表
    public static final String tenant_jrtt_dmp_custom_audience_select_v2 = "parametric_test:tenant_jrtt_dmp_custom_audience_select_v2";//人群包列表/gjx
    public static final String tenant_jrtt_dmp_custom_audience_read_v2 = "parametric_test:tenant_jrtt_dmp_custom_audience_read_v2";//人群包详细信息lzy/依赖人群包列表
    //第一阶段/工具管理5
    public static final String tenant_jrtt_tools_site_v2 = "parametric_test:tenant_jrtt_tools_site_v2";//获取站点列表（获取橙子建站站点列表）
    public static final String tenant_jrtt_tools_image_v2 = "parametric_test:tenant_jrtt_tools_image_v2";//获取图片素材
    public static final String tenant_jrtt_tools_video_v2 = "parametric_test:tenant_jrtt_tools_video_v2";//获取视频素材
    public static final String jrtt_tools_video_cover_suggest_v2 = "parametric_test:jrtt_tools_video_cover_suggest_v2";//获取视频智能封面
//    public static final String tenant_jrtt_tools_site_v2 = "tenant_jrtt_tools_site_v2";//获取站点列表（获取橙子建站站点列表）
//    public static final String tenant_jrtt_tools_image_v2 = "tenant_jrtt_tools_image_v2";//获取图片素材
//    public static final String tenant_jrtt_tools_video_v2 = "tenant_jrtt_tools_video_v2";//获取视频素材
//    public static final String jrtt_tools_video_cover_suggest_v2 = "jrtt_tools_video_cover_suggest_v2";//获取视频智能封面
    public static final String tenant_jrtt_tools_creative_word_v2 = "parametric_test:tenant_jrtt_tools_creative_word_v2";//获取动态创意词包
    public static final String tenant_jrtt_tools_industry_v2 = "parametric_test:tenant_jrtt_tools_industry_v2";//获取行业列表
    public static final String tenant_jrtt_tools_region_v2 = "parametric_test:tenant_jrtt_tools_region_v2";//获取地域列表
    public static final String tenant_jrtt_tools_playable_list_v2 = "parametric_test:tenant_jrtt_tools_playable_list_v2";//获取试玩素材列表
    public static final String tenant_jrtt_tools_advertiser_package_v2 = "parametric_test:tenant_jrtt_tools_advertiser_package_v2";//查询极速下载列表
    public static final String tenant_jrtt_tools_action_category_v2 = "parametric_test:tenant_jrtt_tools_action_category_v2";//行为类目查询
    public static final String tenant_jrtt_tools_interest_category_v2 = "parametric_test:tenant_jrtt_tools_interest_category_v2";//兴趣类目查询
    //    public static final String tenant_jrtt_tools_interest_category_v2 = "tenant_jrtt_tools_interest_category_v2";//兴趣类目查询
    public static final String tenant_jrtt_tools_union_flow_package_v2 = "parametric_test:tenant_jrtt_tools_union_flow_package_v2";//获取穿山甲流量包
    public static final String tenant_jrtt_tools_adv_convert_v2 = "parametric_test:tenant_jrtt_tools_adv_convert_v2";//转化ID列表
    public static final String tenant_jrtt_tools_ad_convert_read_v2 = "parametric_test:tenant_jrtt_tools_ad_convert_read_v2";//查询转化详细信息
    public static final String tenant_jrtt_tools_interest_tags_v2 = "parametric_test:tenant_jrtt_tools_interest_tags_v2";//获取兴趣关键词词包
    public static final String tenant_jrtt_audience_package_v2 = "parametric_test:tenant_jrtt_audience_package_v2";//获取定向包
    //第一阶段/数据报表二期6
    public static final String tenant_jrtt_report_advertiser_audience_province_v2 = "parametric_test:tenant_jrtt_report_advertiser_audience_province_v2";//受众分析数据-省级数据-广告主
    public static final String tenant_jrtt_report_campaign_audience_province_v2 = "parametric_test:tenant_jrtt_report_campaign_audience_province_v2";//受众分析数据-省级数据-广告组
    public static final String tenant_jrtt_report_ad_audience_province_v2 = "parametric_test:tenant_jrtt_report_ad_audience_province_v2";//受众分析数据-省级数据-广告计划
    public static final String tenant_jrtt_report_advertiser_audience_gender_v2 = "parametric_test:tenant_jrtt_report_advertiser_audience_gender_v2";//受众分析数据-性别数据-广告主
    public static final String tenant_jrtt_report_campaign_audience_gender_v2 = "parametric_test:tenant_jrtt_report_campaign_audience_gender_v2";//受众分析数据-性别数据-广告组
    public static final String tenant_jrtt_report_ad_audience_gender_v2 = "parametric_test:tenant_jrtt_report_ad_audience_gender_v2";//受众分析数据-性别数据-广告计划
    public static final String tenant_jrtt_report_advertiser_audience_tag_v2 = "parametric_test:tenant_jrtt_report_advertiser_audience_tag_v2";//受众分析数据-兴趣数据-广告主
    public static final String tenant_jrtt_report_campaign_audience_tag_v2 = "parametric_test:tenant_jrtt_report_campaign_audience_tag_v2";//受众分析数据-兴趣数据-广告组
    public static final String tenant_jrtt_report_ad_audience_tag_v2 = "parametric_test:tenant_jrtt_report_ad_audience_tag_v2";//受众分析数据-兴趣数据-广告计划
    public static final String tenant_jrtt_report_advertiser_audience_age_v2 = "parametric_test:tenant_jrtt_report_advertiser_audience_age_v2";//受众分析数据-年龄数据-广告主
    public static final String tenant_jrtt_report_campaign_audience_age_v2 = "parametric_test:tenant_jrtt_report_campaign_audience_age_v2";//受众分析数据-年龄数据-广告组
    public static final String tenant_jrtt_report_ad_audience_age_v2 = "parametric_test:tenant_jrtt_report_ad_audience_age_v2";//受众分析数据-年龄数据-广告计划
    public static final String tenant_jrtt_report_advertiser_audience_city_v2 = "parametric_test:tenant_jrtt_report_advertiser_audience_city_v2";//受众分析数据-市级数据-广告主
    public static final String tenant_jrtt_report_campaign_audience_city_v2 = "parametric_test:tenant_jrtt_report_campaign_audience_city_v2";//受众分析数据-市级数据-广告组
    public static final String tenant_jrtt_report_ad_audience_city_v2 = "parametric_test:tenant_jrtt_report_ad_audience_city_v2";//受众分析数据-市级数据-广告计划

}
