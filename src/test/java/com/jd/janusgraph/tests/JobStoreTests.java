package com.jd.janusgraph.tests;

import com.jd.janus.model.Job;
import com.jd.janus.store.impl.JobGraphStore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class JobStoreTests {
    private JobGraphStore jobGraphStore = new JobGraphStore();

    @Test
    public void testInsert(){
        Job job = new Job()
                .setId("jiangzz.JCW_ODM_CF_YQGQ_COUPON_RECORD_I_D@5k")
                .setAlias("白条优惠券使用记录（有券、灌券）")
                .setName("JCW_ODM_CF_YQGQ_COUPON_RECORD_I_D")
                .setType("离线作业")
                .setStatus("下线")
                .setRunDate(new Date())
                .setLeadingCadre("bjmengsongjie")
                .setPersonInCharge(Arrays.asList("bjmengsongjie","guoning14"))
                .setUrl("http://www.baidu.com");

        jobGraphStore.insert(job);
    }
    @Test
    public void testQuery(){
        Job job = jobGraphStore.queryByUniqueAttr("id", "jiangzz.JCW_ODM_CF_YQGQ_COUPON_RECORD_I_D@5k");
        System.out.println(job);
    }
    @Test
    public void testDelete(){
        jobGraphStore.delete("type","离线作业");
    }

    @Test
    public void testUpdate(){
        HashMap<String, Object> objectValue = new HashMap<String, Object>();
        objectValue.put("personInCharge",Arrays.asList("王小五","赵小龙"));
        objectValue.put("runDate",new Date());
        objectValue.put("type","离线作业");
        objectValue.put("createTime",new Date());
        jobGraphStore.updateByQuery("id","jiangzz.JCW_ODM_CF_YQGQ_COUPON_RECORD_I_D@5k", objectValue);
    }
    @Test
    public void testExists(){
        Boolean exits = jobGraphStore.exits("id","jiangzz.JCW_ODM_CF_YQGQ_COUPON_RECORD_I_D@5k");
        System.out.println(exits);
    }
    @Test
    public void testUpsert(){
        Job job = new Job()
                .setId("jiangzz.JCW_ODM_CF_YQGQ_COUPON_RECORD_H_D@5k")
                .setAlias("白条优惠券使用记录（有券、灌券）")
                .setName("JCW_ODM_CF_YQGQ_COUPON_RECORD_H_D")
                .setType("离线作业")
                .setStatus("下线")
                .setRunDate(new Date())
                .setLeadingCadre("bjmengsongjie")
                .setPersonInCharge(Arrays.asList("bjmengsongjie","guoning14"))
                .setUrl("http://www.baidu.com");
        jobGraphStore.upsert(job);
    }

    @Test
    public void testQueryList(){
        List<Job> jobs = jobGraphStore.queryByAttr("type", "离线作业", 2L, 0L);
        for (Job job : jobs) {
            System.out.println(job);
        }
    }
}
