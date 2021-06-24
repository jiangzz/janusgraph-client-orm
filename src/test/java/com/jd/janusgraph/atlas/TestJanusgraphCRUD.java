package com.jd.janusgraph.atlas;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.core.attribute.Text.HAS_CONTAINS;
import static org.janusgraph.core.attribute.Text.textContains;

public class TestJanusgraphCRUD {
    private JanusGraph janusGraph;
    private JanusGraph janusGraph2;
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

        JanusGraphFactory.Builder builder2 = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "apache_atlas_janus")
                .set("index.search.backend", "elasticsearch")
                .set("schema.constraints",true)
                .set("schema.default","none")
                .set("index.search.hostname", "CentOS");

        janusGraph = builder.open();
        janusGraph2 = builder2.open();
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
    public void testQueryLineage2(){
        GraphTraversalSource traversal = janusGraph.traversal();
        Long count = traversal.V()
                .or(__.has("__typeName", P.eq("hive_process")), __.has("__typeName", P.eq("hive_process_execution")), __.has("__typeName", P.eq("hive_column_lineage")))
                .has("__timestamp", P.gte(1624413445112L))
                .count()
                .next();
        System.out.println(count);
    }
    @Test
    public void testQueryLineage() throws InterruptedException {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(8, 16, 1000, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setDaemon(false).build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        GraphTraversalSource traversal = janusGraph.traversal();
        GraphTraversal<Vertex, Vertex> g = traversal.V()
                .or(__.has("__typeName", P.eq("hive_process")), __.has("__typeName", P.eq("hive_process_execution")), __.has("__typeName", P.eq("hive_column_lineage")));
        int size=3;
        int page=1;
        while (true){
            System.out.println("开始:"+page++ +"数据");
            List<Vertex> vertexs = g.next(page);
            if(vertexs.size()>0){
                CountDownLatch countDownLatch = new CountDownLatch(vertexs.size());
                vertexs.forEach(v->{
                    if((Long)v.property("__timestamp").value() <=1000L )
                    pool.submit(()->{
                        Graph graph=null;
                        try {
                            System.out.println("开始删除:"+v);
                            graph = janusGraph.tx().createThreadedTx();
                            graph.traversal().V(v).drop().iterate();
                            graph.tx().commit();
                            System.out.println("删除成功:"+v);
                            countDownLatch.countDown();
                        } catch (Exception e) {
                            countDownLatch.countDown();
                            if(graph!=null) graph.tx().rollback();
                        }
                    });
                });
                countDownLatch.await();
            }else {
                break;
            }
        }

    }

    @Test
    @SneakyThrows
    public void testHbase(){

        Configuration conf = new Configuration();
        conf.set(HConstants.ZOOKEEPER_QUORUM,"CentOS:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("default:apache_atlas_janus");
        Table table = conn.getTable(tname);
        Scan scan = new Scan();
        scan.setLimit(100);
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs) {
            while (result.advance()){
                Cell cell = result.current();
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));
                long ts=cell.getTimestamp();
                System.out.println(row+"=>"+cf+":"+col+"\t"+v+" ts:"+ts);
            }
        }
        table.close();

    }
    @Test
    public void testDropLineage(){
        System.out.println("========删除=========");
        GraphTraversalSource g = janusGraph.traversal();
        g.V()
         .or(__.has("__typeName", P.eq("hive_process")), __.has("__typeName", P.eq("hive_process_execution")), __.has("__typeName", P.eq("hive_column_lineage")))
          .limit(1).drop().iterate();
        g.tx().commit();
    }
    @Test
    public void testQueryLineageEage(){
        GraphTraversalSource traversal = janusGraph.traversal();
        List<Vertex> edges = traversal.V()
                .has("__typeName","hive_process")
                .limit(25)
                .toList();
        for (Vertex edge : edges) {
            System.out.println("========================");
            Set<String> keys = edge.keys();
            for (String key : keys) {
                System.out.println(key+"\t"+edge.properties(key).next().value());
            }
        }

    }
    @After
    public void after(){
        janusGraph.close();
    }
}
