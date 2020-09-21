package com.example.gdt;

import com.example.jrtt.HbaseDelete;

import java.io.IOException;
import java.util.ArrayList;

public class gdtTestRDD {

    public static void main(String[] args) throws IOException {
        //遍历清空表
        HbaseDelete delete = new HbaseDelete();
        ArrayList<String> tableList = new ArrayList<>();
//★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★test-adv-1,2,3★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
//        //广告主列表
//        tableList.add("parametric_test:tenant_gdt_advertiser_v2");
        //获取广告主资质
        tableList.add("parametric_test:tenant_gdt_qualifications_v2");
        //遍历删除
        for (int i = 0; i <tableList.size(); i++) {
            delete.DeleteData(tableList.get(i));
        }

    }
}
