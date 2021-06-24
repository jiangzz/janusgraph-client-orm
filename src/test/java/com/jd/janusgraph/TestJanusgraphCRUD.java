package com.jd.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestJanusgraphCRUD {
    private JanusGraph janusGraph;
    @Before
    public void before(){
        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "apache_atlas_janus")
                .set("index.search.backend", "elasticsearch")
                .set("schema.constraints",true)
                .set("schema.default","none")
                .set("index.search.hostname", "CentOS");

        janusGraph = builder.open();
    }

    @Test
    public void testInsertData01() throws BackendException {
        JanusGraphVertex v1 = janusGraph.addVertex(T.label, "person", "name", "蒋中洲", "sex", true, "birthDay", new Date());
        JanusGraphVertex v2 = janusGraph.addVertex(T.label, "person", "name", "蒋泽宇", "sex", true, "birthDay", new Date());
        v2.addEdge("father",v1,"name","爸爸");
        janusGraph.tx().commit();
    }
    @Test
    public void testInsertData02() throws BackendException {
        GraphTraversalSource g = janusGraph.traversal();
        Vertex v2 = g.V().hasLabel("person").has("name", "蒋泽宇").next();
        JanusGraphVertex v3 = janusGraph.addVertex(T.label, "person", "name", "王文玉", "sex", false, "birthDay", new Date());
        if(v2!=null){
            v2.addEdge("mother",v3,"name","妈妈");
        }
        janusGraph.tx().commit();
    }
    @Test
    public void testQueryData() throws BackendException {
        GraphTraversalSource g = janusGraph.traversal();
        Vertex vertex = g.V().hasLabel("person").has("name", "蒋泽宇").next();
        for (String key : vertex.keys()) {
            System.out.println(key+"\t"+vertex.property(key).value());
        }
        g.tx().commit();
    }
    @Test
    public void testUpdateData3() throws BackendException {
        GraphTraversalSource g = janusGraph.traversal();
        Vertex v2 = g.V().hasLabel("person").has("name", "蒋中洲").next();
        Vertex v3 = janusGraph.addVertex(T.label, "city", "name", "北京");
        v2.addEdge("lives",v3,"name","工作占时居住");
        janusGraph.tx().commit();
    }
    @Test
    public void testUpdateData4() throws BackendException {
        GraphTraversalSource g = janusGraph.traversal();
        Vertex v2 = g.V().hasLabel("city").has("name", "北京").next();
        Vertex v3 = g.V().hasLabel("person").has("name", "王文玉").next();
        v3.addEdge("lives",v2,"name","工作占时居住");
        janusGraph.tx().commit();
    }

    //查询居住在北京的人
    @Test
    public void testQueryCity() throws BackendException {
        GraphTraversalSource g = janusGraph.traversal();
        List<Vertex> vertices = g.V().hasLabel("city").has("name", "北京")
                .in("lives")
                .in("mother")
                .toList();

        for (Vertex vertex : vertices) {
            for (String key : vertex.keys()) {
                System.out.println(key+"\t"+vertex.property(key).value());
            }
        }
        janusGraph.tx().commit();
    }
    @Test
    public void testUpdateProperty() throws BackendException {
        GraphTraversalSource g = janusGraph.traversal();
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();

        Vertex vertex = g.V().hasLabel("city").has("name", "北京").next();
        VertexProperty<Object> p = vertex.property("name");
         p.property("name","王五");

        g.tx().commit();
    }

    @After
    public void after(){
        janusGraph.close();
    }
}
