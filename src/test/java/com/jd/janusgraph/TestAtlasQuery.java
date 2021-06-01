package com.jd.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.janusgraph.core.attribute.Text.*;
import java.util.List;
import java.util.stream.Collectors;

public class TestAtlasQuery {
    private JanusGraph janusGraph;
    @Before
    public void before(){
        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "apache_atlas_janus")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "CentOS");

        janusGraph = builder.open();
    }
    @Test
    public void showSchema(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //全量信息，涵盖edge、vertex、property、index等
        String schema = mgmt.printSchema();
        System.out.println(schema);
        mgmt.commit();
    }
    @Test
    public void queryFullTextSearchTableName1(){
        GraphTraversalSource traversal = janusGraph.traversal();
        List<Vertex> vertices = traversal.V()
                .has("__typeName","hive_table")
                .has("Asset.__s_name", textContains("user")).limit(25)
                .toList();
        for (Vertex vertex : vertices) {
            System.out.println(vertex.property("Asset.__s_name"));
        }
    }
    @Test
    public void queryFullTextSearchTableName(){
        GraphTraversalSource traversal = janusGraph.traversal();
        List<Vertex> vertices = traversal.V()
                .has("__typeName","hive_table")
                .has("Asset.__s_name", textContains("user")).limit(25)
                .toList();
        for (Vertex vertex : vertices) {
            System.out.println(vertex.property("Asset.__s_name"));
        }
    }
    @Test
    public void queryStringSearchTableName(){
        GraphTraversalSource traversal = janusGraph.traversal();
        List<Vertex> vertices = traversal.V()
                .has("__typeName","hive_table")
                .has("Asset.__s_name",eq("t_user_p") ).limit(25)
                .toList();
        for (Vertex vertex : vertices) {
            System.out.println(vertex.property("Asset.__s_name"));
        }
    }
    @Test
    public void testDirectoryQuery(){
        List<JanusGraphIndexQuery.Result<JanusGraphVertex>> list = janusGraph
                .indexQuery("vertex_index", "Asset.__s_name:t_user* && __typeName:hive_table")
                .limit(2).offset(0)
                .vertexStream().collect(Collectors.toList());

        list.stream().forEach(v->{
            JanusGraphVertex vertex = v.getElement();
            double score = v.getScore();
            System.out.println(vertex.property("Asset.__s_name")+" score:"+score);
        });

    }
    @After
    public void after(){
        janusGraph.close();
    }
}
