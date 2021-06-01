package com.jd.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class TestJanusgraphSchema_prod {
    private JanusGraph janusGraph;
    @Before
    public void before(){

        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "jtlas")
                .set("schema.constraints",true)
                .set("schema.default","none")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "CentOS")
                .set("index.search.elasticsearch.http.auth.type", "custom")
                .set("index.search.elasticsearch.http.auth.custom.authenticator-class", "com.jd.janusgraph.JESRestClientAuthenticator")
                .set("index.search.elasticsearch.http.auth.custom.authenticator-args", "c9f03dff789318d91b35ef0fc26fed33,galaxy-jtlas-app");

        janusGraph = builder.open();

    }

    @Test
    public void testCreateProdSchema() throws InterruptedException {
        JanusGraphManagement mgmt = janusGraph.openManagement();

        //创建顶点
        VertexLabel dataModel = mgmt.makeVertexLabel("DATA_MODEL").make();
        VertexLabel job = mgmt.makeVertexLabel("JOB").make();
        VertexLabel quota = mgmt.makeVertexLabel("QUOTA").make();
        VertexLabel topic = mgmt.makeVertexLabel("TOPIC").make();
        VertexLabel quotaSys= mgmt.makeVertexLabel("QUOTA_SYS").make();

        //创建边
        EdgeLabel data_model_2_job = mgmt.makeEdgeLabel("DATA_MODEL_2_JOB").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel job_2_data_model = mgmt.makeEdgeLabel("JOB_2_DATA_MODEL").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel data_model_2_quota = mgmt.makeEdgeLabel("DATA_MODEL_2_QUOTA").multiplicity(Multiplicity.ONE2MANY).make();
        EdgeLabel quota_2_quota = mgmt.makeEdgeLabel("QUOTA_2_QUOTA").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel topic_2_quota = mgmt.makeEdgeLabel("TOPIC_2_QUOTA").multiplicity(Multiplicity.ONE2MANY).make();
        EdgeLabel topic_2_job = mgmt.makeEdgeLabel("TOPIC_2_JOB").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel job_2_topic = mgmt.makeEdgeLabel("JOB_2_TOPIC").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel topic_2_quota_sys = mgmt.makeEdgeLabel("TOPIC_2_QUOTA_SYS").multiplicity(Multiplicity.ONE2MANY).make();


        PropertyKey id = mgmt.makePropertyKey("id").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey cluster = mgmt.makePropertyKey("cluster").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey database = mgmt.makePropertyKey("database").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey alias = mgmt.makePropertyKey("alias").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey leadingCadre = mgmt.makePropertyKey("leadingCadre").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey personInCharge = mgmt.makePropertyKey("personInCharge").dataType(String.class).cardinality(Cardinality.LIST).make();
        PropertyKey usageDescription = mgmt.makePropertyKey("usageDescription").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey businessDescription = mgmt.makePropertyKey("businessDescription").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey url = mgmt.makePropertyKey("url").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey type = mgmt.makePropertyKey("type").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey createTime = mgmt.makePropertyKey("createTime").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey runDate = mgmt.makePropertyKey("runDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey status = mgmt.makePropertyKey("status").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey topicLayer = mgmt.makePropertyKey("topicLayer").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey code = mgmt.makePropertyKey("code").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey businessDirector = mgmt.makePropertyKey("businessDirector").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey technicalDirector = mgmt.makePropertyKey("technicalDirector").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey objectId = mgmt.makePropertyKey("objectId").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey objectDescription = mgmt.makePropertyKey("objectDescription").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        PropertyKey from = mgmt.makePropertyKey("from").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey to = mgmt.makePropertyKey("to").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        //设置索引属性
        mgmt.buildIndex("_id_vertex_Index", Vertex.class).addKey(id).unique().buildCompositeIndex();
        mgmt.buildIndex("cluster_vertex_index", Vertex.class).addKey(cluster).buildCompositeIndex();
        mgmt.buildIndex("database_vertex_index", Vertex.class).addKey(database).buildCompositeIndex();
        mgmt.buildIndex("name_vertex_index", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.buildIndex("alias_vertex_index", Vertex.class).addKey(alias).buildCompositeIndex();
        mgmt.buildIndex("leadingCadre_vertex_index", Vertex.class).addKey(leadingCadre).buildCompositeIndex();
        mgmt.buildIndex("personInCharge_vertex_index", Vertex.class).addKey(personInCharge).buildCompositeIndex();
        mgmt.buildIndex("type_vertex_index", Vertex.class).addKey(type).buildCompositeIndex();
        mgmt.buildIndex("status_vertex_index", Vertex.class).addKey(status).buildCompositeIndex();
        mgmt.buildIndex("topicLayer_vertex_index", Vertex.class).addKey(topicLayer).buildCompositeIndex();
        mgmt.buildIndex("code_vertex_index", Vertex.class).addKey(code).buildCompositeIndex();
        mgmt.buildIndex("businessDirector_vertex_index", Vertex.class).addKey(businessDirector).buildCompositeIndex();
        mgmt.buildIndex("technicalDirector_vertex_index", Vertex.class).addKey(technicalDirector).buildCompositeIndex();
        mgmt.buildIndex("objectId_vertex_index", Vertex.class).addKey(objectId).buildCompositeIndex();


        mgmt.buildIndex("createTime_mixed_vertex_index", Vertex.class).addKey(createTime).buildMixedIndex("search");
        mgmt.buildIndex("runDate_mixed_vertex_index", Vertex.class).addKey(runDate).buildMixedIndex("search");
        mgmt.buildIndex("name_mixed_vertex_index", Vertex.class).addKey(name).buildMixedIndex("search");
        mgmt.buildIndex("alias_mixed_vertex_index", Vertex.class).addKey(alias).buildMixedIndex("search");
        mgmt.buildIndex("usageDescription_mixed_vertex_index", Vertex.class).addKey(usageDescription).buildMixedIndex("search");
        mgmt.buildIndex("businessDescription_mixed_vertex_index", Vertex.class).addKey(businessDescription).buildMixedIndex("search");
        mgmt.buildIndex("objectDescription_mixed_vertex_index", Vertex.class).addKey(objectDescription).buildMixedIndex("search");


        mgmt.buildIndex("_id_edge_index", Edge.class).addKey(id).buildCompositeIndex();
        mgmt.buildIndex("from_edge_index", Edge.class).addKey(from).buildCompositeIndex();
        mgmt.buildIndex("to_edge_index", Edge.class).addKey(to).buildCompositeIndex();
        mgmt.buildIndex("createTime_mixed_edge_index", Edge.class).addKey(createTime).buildMixedIndex("search");


        //给 定点 添加属性
        mgmt.addProperties(dataModel,id,cluster,database,name,alias,leadingCadre,personInCharge,usageDescription,businessDescription,url);
        mgmt.addProperties(job,id,type,name,alias,createTime,runDate,leadingCadre,personInCharge,status,url);
        mgmt.addProperties(topic,id,type,topicLayer,name,alias,usageDescription,businessDescription,leadingCadre,personInCharge,url);
        mgmt.addProperties(quota,id,type,code,name,alias,businessDirector,technicalDirector,businessDescription,url);
        mgmt.addProperties(quotaSys,id,objectId,alias,objectDescription,personInCharge,url);

        //给边添加属性
        mgmt.addProperties(data_model_2_job,id,from,to,createTime);
        mgmt.addProperties(job_2_data_model,id,from,to,createTime);
        mgmt.addProperties(data_model_2_quota,id,from,to,createTime);
        mgmt.addProperties(topic_2_job,id,from,to,createTime);
        mgmt.addProperties(job_2_topic,id,from,to,createTime);
        mgmt.addProperties(topic_2_quota,id,from,to,createTime);
        mgmt.addProperties(topic_2_quota_sys,id,from,to,createTime);
        mgmt.addProperties(quota_2_quota,id,from,to,createTime);

        //创建一顶点为核心的索引
        mgmt.buildEdgeIndex(data_model_2_job,"createTime", Direction.BOTH, Order.desc,createTime);
        mgmt.buildEdgeIndex(job_2_data_model,"createTime", Direction.BOTH, Order.desc,createTime);

        mgmt.buildEdgeIndex(data_model_2_quota,"createTime", Direction.BOTH, Order.desc,createTime);

        mgmt.buildEdgeIndex(topic_2_job,"createTime", Direction.BOTH, Order.desc,createTime);
        mgmt.buildEdgeIndex(job_2_topic,"createTime", Direction.BOTH, Order.desc,createTime);

        mgmt.buildEdgeIndex(topic_2_quota,"createTime", Direction.BOTH, Order.desc,createTime);
        mgmt.buildEdgeIndex(topic_2_quota_sys,"createTime", Direction.BOTH, Order.desc,createTime);
        mgmt.buildEdgeIndex(quota_2_quota,"createTime", Direction.BOTH, Order.desc,createTime);


        //创建链接参数链接参数
        mgmt.addConnection(data_model_2_job,dataModel,job);
        mgmt.addConnection(job_2_data_model,job,dataModel);
        mgmt.addConnection(data_model_2_quota,dataModel,quota);
        mgmt.addConnection(topic_2_job,topic,job);
        mgmt.addConnection(job_2_topic,job,topic);
        mgmt.addConnection(topic_2_quota,topic,quota);
        mgmt.addConnection(topic_2_quota_sys,topic,quotaSys);
        mgmt.addConnection(quota_2_quota,quota,quota);

        mgmt.commit();

    }

    @Test
    public void testShowSchema(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //全量信息，涵盖edge、vertex、property、index等
        String schema = mgmt.printSchema();
        System.out.println(schema);
        mgmt.commit();
    }

    @Test
    public void dropSchema() throws BackendException {
      JanusGraphFactory.drop(janusGraph);
    }

    @Test

    public void testInserDataModelData() throws Exception {

        FileInputStream fis=new FileInputStream("E:\\IdeaProject\\janusgraph-client-atlas\\src\\main\\resources\\数据模型顶点.csv");
        InputStreamReader inputStreamReader = new InputStreamReader(fis,"utf-8");
        BufferedReader br = new BufferedReader(inputStreamReader);
        Integer count=0;
        String line=null;
        while ((line=br.readLine())!=null){
            String[] split = line.split("\t");
            janusGraph.addVertex(T.label,"DATA_MODEL","_id",split[1],"cluster",split[2],"database",split[3],"name",split[4],"alias",split[5],"leadingCadre",split[6],"personInCharge",split[7],"usageDescription",split[8],"businessDescription",split[9],"url",split[10]);
            count++;
            if(count%1000==0 && count!=0){
                System.out.println("提交一次事物！");
                janusGraph.tx().commit();
            }

        }
        janusGraph.tx().commit();

    }
    @Test
    public void testQueryData() throws Exception {

        GraphTraversalSource traversal = janusGraph.traversal();
        Long cluster = traversal.V().has("cluster", "5k").count().next();
        Vertex vertex = traversal.V().has("_id", "tape_sdm.dws_brs_jr_flow_sum_i_d@5k").next();
        for (String key : vertex.keys()) {
            System.out.println(key+"\t"+vertex.property(key).value());
        }
    }
    @Test
    public void testDropData() throws Exception {
        GraphTraversalSource traversal = janusGraph.traversal();
        Long count = traversal.V().has("cluster", "5k").count().next();
        while(count!=0){
            List<Vertex> cluster = traversal.V().has("cluster", "5k").limit(1000).toList();
            for (Vertex vertex : cluster) {
                traversal.V(vertex).drop().iterate();
            }

            count = traversal.V().has("cluster", "5k").count().next();
        }
        traversal.tx().commit();
    }
    @Test
    public void testQueryDataCount() throws Exception {
        GraphTraversalSource traversal = janusGraph.traversal();
        Long count = traversal.V().has("cluster", "5k").count().next();
        System.out.println(count);
    }

    @After
    public void after(){
        janusGraph.close();
    }
}
