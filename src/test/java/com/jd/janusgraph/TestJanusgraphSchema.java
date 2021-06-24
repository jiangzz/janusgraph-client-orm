package com.jd.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.janusgraph.core.attribute.Text.textContains;

public class TestJanusgraphSchema {
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
                .set("index.search.elasticsearch.http.auth.custom.authenticator-class", "com.jd.janus.auth.JESRestClientAuthenticator")
                .set("index.search.elasticsearch.http.auth.custom.authenticator-args", "c9f03dff789318d91b35ef0fc26fed33,galaxy-jtlas-app");
        janusGraph = builder.open();
    }
    @Test
    public void testDirectoryQuery(){
        ConfigOption<String> esHttpAuthenticatorClass = ElasticSearchIndex.ES_HTTP_AUTHENTICATOR_CLASS;
        System.out.println(esHttpAuthenticatorClass);
        List<JanusGraphIndexQuery.Result<JanusGraphVertex>> list = janusGraph
                .indexQuery("c_person_vertex_index", "Asset.__s_name:t_user* && __typeName:hive_table")
                .limit(2).offset(0)
                .vertexStream().collect(Collectors.toList());

        list.stream().forEach(v->{
            JanusGraphVertex vertex = v.getElement();
            double score = v.getScore();
            System.out.println(vertex.property("Asset.__s_name")+" score:"+score);
        });

    }
    @Test
    public void testCreateSchema2() throws InterruptedException {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        EdgeLabel mother = mgmt.getEdgeLabel("mother");
        VertexLabel person = mgmt.getVertexLabel("person");
        mgmt.addConnection(mother,person,person);
        mgmt.commit();
    }
    @Test
    public void testCreateSchema() throws InterruptedException {
        JanusGraphManagement mgmt = janusGraph.openManagement();

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel city = mgmt.makeVertexLabel("city").make();

        EdgeLabel father = mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel mother = mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel lives = mgmt.makeEdgeLabel("lives").multiplicity(Multiplicity.MULTI).make();


        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey birthDay = mgmt.makePropertyKey("birthDay").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey sex = mgmt.makePropertyKey("sex").dataType(Boolean.class).cardinality(Cardinality.SINGLE).make();


        //给定点添加属性
        mgmt.addProperties(person,name);
        mgmt.addProperties(person,birthDay);
        mgmt.addProperties(person,sex);

        //给边添加属性
        mgmt.addProperties(mother,name);
        mgmt.addProperties(father,name);
        mgmt.addProperties(lives,name);

        //给location添加属性
        mgmt.addProperties(city,name);

        //添加边关系属性
        mgmt.addConnection(father,person,person);
        mgmt.addConnection(mother,person,person);
        mgmt.addConnection(lives,person,city);

        //设置索引属性
        mgmt.buildIndex("c_name_vertex_index", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.buildIndex("c_birthDay_vertex_index", Vertex.class).addKey(birthDay).buildCompositeIndex();
        mgmt.buildIndex("c_sex_vertex_index", Vertex.class).addKey(sex).buildCompositeIndex();
        mgmt.buildIndex("c_name_edge_index", Edge.class).addKey(name).buildMixedIndex("search");

        mgmt.buildIndex("c_person_vertex_index",Vertex.class)
                .addKey(name)
                .addKey(birthDay)
                .addKey(sex)
                .buildMixedIndex("search");
        mgmt.buildIndex("m_name_edge_index",Edge.class)
                .addKey(name)
                .buildMixedIndex("search");

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
    @After
    public void after(){
        janusGraph.close();
    }
}
