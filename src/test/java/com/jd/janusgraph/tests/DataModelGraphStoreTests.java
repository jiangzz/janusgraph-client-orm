package com.jd.janusgraph.tests;

import com.jd.janus.model.DataModel;
import com.jd.janus.model.Job;
import com.jd.janus.store.impl.DataModelGraphStore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DataModelGraphStoreTests {
    private DataModelGraphStore basiscGraphStore = new DataModelGraphStore();

    @Test
    public void testInsert(){
        DataModel dataModel = new DataModel()
                .setId("jiangzz.t_user@5k")
                .setAlias("用户表")
                .setName("t_user")
                .setBusinessDescription("用户表存储用户基本信息")
                .setCluster("5k")
                .setDatabase("jiangzz")
                .setLeadingCadre("蒋中洲")
                .setPersonInCharge(Arrays.asList("张三","李四"))
                .setUsageDescription("用户表存储用户基本信息")
                .setUrl("http://www.baidu.com");

        basiscGraphStore.insert(dataModel);
    }
    @Test
    public void testDelete(){
        basiscGraphStore.delete("id","jiangzz.t_user@5k");
    }
    @Test
    public void testQuery(){
        DataModel dataModel = basiscGraphStore.queryByUniqueAttr("id", "jiangzz.t_user@5k");
        System.out.println(dataModel);
    }
    @Test
    public void testUpdate(){
        HashMap<String, Object> objectValue = new HashMap<String, Object>();
        objectValue.put("personInCharge",Arrays.asList("王小五","赵小龙"));
        objectValue.put("leadingCadre","王小五");
        objectValue.put("url","http://www.baidu.com");
        basiscGraphStore.updateByQuery("id","jiangzz.t_user@5k", objectValue);
    }
    @Test
    public void testUpsert(){
        DataModel dataModel = new DataModel()
                .setId("jiangzz.t_order@5k")
                .setAlias("订单表数据")
                .setName("t_order")
                .setBusinessDescription("用户表存储用户基本信息")
                .setCluster("5k")
                .setDatabase("jiangzz")
                .setLeadingCadre("昂小")
                .setPersonInCharge(Arrays.asList("张三","赵小龙"))
                .setUsageDescription("用户表存储用户基本信息111")
                .setUrl("http://www.baidu.com");
        basiscGraphStore.upsert(dataModel);
    }
    @Test
    public void testExists(){
        Boolean exits = basiscGraphStore.exits("name", "t_user");
        System.out.println(exits);
    }

    @Test
    public void testQueryList(){
        List<DataModel> models = basiscGraphStore.queryByAttr("cluster", "5k", 2L, 0L);
        for (DataModel model : models) {
            System.out.println(model);
        }
    }
    @Test
    public void testAddEdge(){
        DataModel dataModel = new DataModel().setId("jiangzz.t_user@5k");
        Job job = new Job().setId("jiangzz.JCW_ODM_CF_YQGQ_COUPON_RECORD_I_D@5k");
        basiscGraphStore.addRelationship(dataModel,job,"DATA_MODEL_2_JOB",false);
    }
}
