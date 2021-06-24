package com.jd.janus.store;

import com.google.gson.annotations.SerializedName;
import com.jd.janus.annotation.AtlasVertex;
import com.jd.janus.annotation.AtlasProperty;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;

@Slf4j
public abstract class GraphStore<T> {
    private JanusGraph janusGraph;
    private static final String PRFIEX = "jtlas";

    private Class targetClass;

    public GraphStore(Configuration conf) {
        targetClass = (Class) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];

        boolean present = targetClass.isAnnotationPresent(AtlasVertex.class);
        if(!present){
            throw new RuntimeException(String.format("实体%s必须指定%s注解", targetClass.getSimpleName(), "@AtlasVertex"));
        }

        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "jtlas")
                .set("schema.constraints", true)
                .set("schema.default", "none")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "CentOS")
                .set("index.search.elasticsearch.http.auth.type", "custom")
                .set("index.search.elasticsearch.http.auth.custom.authenticator-class", "com.jd.janus.auth.JESRestClientAuthenticator")
                .set("index.search.elasticsearch.http.auth.custom.authenticator-args", "c9f03dff789318d91b35ef0fc26fed33,galaxy-jtlas-app");

        janusGraph = builder.open();
    }
    @SneakyThrows
    public void insert(T t) {
        AtlasVertex graph = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource g = janusGraph.traversal();
        org.apache.tinkerpop.gremlin.structure.Vertex vertex = g.addV(graph.label()).next();
        Field[] declaredFields = targetClass.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            declaredField.setAccessible(true);
            boolean isExists = declaredField.isAnnotationPresent(AtlasProperty.class);
            Object fieldValue = declaredField.get(t);
            String fieldName = declaredField.getName();
            if (isExists) {
                AtlasProperty property = declaredField.getAnnotation(AtlasProperty.class);
                switch (property.cardinality()) {
                    case SINGLE: {
                        if (fieldValue != null) {
                            vertex.property(property.name(), fieldValue);
                        }
                        break;
                    }
                    case SET:
                    case LIST: {
                        if (fieldValue != null) {
                            Collection values = (Collection) fieldValue;
                            for (Object value : values) {
                                vertex.property(property.name(), value);
                            }
                        }
                        break;
                    }
                }
            } else {
                if (fieldValue != null) {
                    if (fieldValue instanceof Collection) {
                        for (Object collectionValue : (Collection) fieldValue) {
                            vertex.property(fieldName, collectionValue);
                        }
                    } else {
                        vertex.property(fieldName, fieldValue);
                    }
                }
            }
        }
        g.tx().commit();
    }

    public void updateByQuery(String attrName, Object value, Map<String, Object> attributes) {
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource traversal = janusGraph.traversal();
        GraphTraversal<org.apache.tinkerpop.gremlin.structure.Vertex, org.apache.tinkerpop.gremlin.structure.Vertex> vertexGraphTraversal = traversal.V().hasLabel(atlasVertex.label()).has(attrName, value);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getValue() instanceof Collection) {
                Collection collectionValues = (Collection) entry.getValue();
                //删除历史集合数据
                traversal.V().hasLabel(atlasVertex.label()).has(attrName, value).properties(entry.getKey()).drop().tryNext();
                for (Object collectionValue : collectionValues) {
                    vertexGraphTraversal = vertexGraphTraversal.property(entry.getKey(), collectionValue);
                }
            } else {
                vertexGraphTraversal = vertexGraphTraversal.property(entry.getKey(), entry.getValue());
            }
        }
        vertexGraphTraversal.iterate();

        traversal.tx().commit();
    }
    @SneakyThrows
    public void update(T t) {
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        String primaryKey = atlasVertex.primaryKey();
        Field primaryFiled = targetClass.getDeclaredField(primaryKey);
        primaryFiled.setAccessible(true);
        Object primaryKeyValue = primaryFiled.get(t);
        if (primaryKey == null) {
            throw new RuntimeException("执行upset必须指定primaryKeyValue值！");
        }
        GraphTraversalSource g = janusGraph.traversal();

        Optional<org.apache.tinkerpop.gremlin.structure.Vertex> vertexOption = g.V().hasLabel(atlasVertex.label()).has(primaryKey, primaryKeyValue).tryNext();
        if (vertexOption.isPresent()) {
            org.apache.tinkerpop.gremlin.structure.Vertex vertex = vertexOption.get();
            Field[] declaredFields = targetClass.getDeclaredFields();
            //迭代所有的key
            for (Field declaredField : declaredFields) {
                declaredField.setAccessible(true);
                Object fieldValue = declaredField.get(t);
                String fieldName = declaredField.getName();
                if (fieldValue == null) {
                    continue;
                }
                boolean isExists = declaredField.isAnnotationPresent(AtlasProperty.class);
                if (isExists) {
                    AtlasProperty propertyAnno = declaredField.getAnnotation(AtlasProperty.class);
                    switch (propertyAnno.cardinality()) {
                        case SINGLE: {
                            vertex.property(propertyAnno.name(), fieldValue);
                            break;
                        }
                        case LIST:
                        case SET: {
                            //删除历史数据
                            g.V().hasLabel(atlasVertex.label()).has(primaryKey, primaryKeyValue).properties(propertyAnno.name()).drop().tryNext();
                            Collection values = (Collection) fieldValue;
                            for (Object value : values) {
                                vertex.property(propertyAnno.name(), value);
                            }
                            break;
                        }
                        default: {
                            //TODO
                        }
                    }
                } else {
                    if (fieldValue instanceof Collection) {
                        //删除历史数据
                        g.V().hasLabel(atlasVertex.label()).has(primaryKey, primaryKeyValue).properties(fieldName).drop().tryNext();
                        for (Object collectionValue : (Collection) fieldValue) {
                            vertex.property(fieldName, collectionValue);
                        }
                    } else {
                        vertex.property(fieldName, fieldValue);
                    }
                }
            }
        }
        g.tx().commit();
    }

    public void delete(String attrName, Object value) {
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource traversal = janusGraph.traversal();
        traversal.V().hasLabel(atlasVertex.label()).has(attrName, value).drop().iterate();
        traversal.tx().commit();
    }

    public void deleteAttribute(String attrName, Object value, String key) {
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource traversal = janusGraph.traversal();
        traversal.V().hasLabel(atlasVertex.label()).has(attrName, value).properties(key).drop().iterate();
        traversal.tx().commit();
    }

    @SneakyThrows
    public T upsert(T t) {
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        String primaryKey = atlasVertex.primaryKey();
        Field primaryField = targetClass.getDeclaredField(primaryKey);
        primaryField.setAccessible(true);
        Object primaryKeyValue = primaryField.get(t);

        if (primaryKey == null || primaryKeyValue == null) {
            throw new RuntimeException("执行upset必须指定primaryKeyValue值！");
        }

        GraphTraversalSource g = janusGraph.traversal();
        Optional<Vertex> vertexOption = g.V().hasLabel(atlasVertex.label()).has(primaryKey, primaryKeyValue).tryNext();
        //更新逻辑
        if (vertexOption.isPresent()) {
            update(t);
        } else {
            //插入逻辑
            insert(t);
        }
        return null;
    }

    public Boolean exits(String attrName, Object value) {
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource g = janusGraph.traversal();
        long count = g.V().hasLabel(atlasVertex.label()).has(attrName, value).count().next().longValue();
        return count != 0;
    }

    @SneakyThrows
    public T queryByUniqueAttr(String attrName, Object value) {
        T targetObject = null;
        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource traversal = janusGraph.traversal();

        Optional<Vertex> vertexOption = traversal.V().hasLabel(atlasVertex.label()).has(attrName, value).tryNext();
        if (vertexOption.isPresent()) {
            org.apache.tinkerpop.gremlin.structure.Vertex vertex = vertexOption.get();
            targetObject = (T) targetClass.newInstance();
            Set<String> keys = vertex.keys();
            Field[] declaredFields = targetClass.getDeclaredFields();
            for (Field declaredField : declaredFields) {
                boolean isExists = declaredField.isAnnotationPresent(AtlasProperty.class);
                declaredField.setAccessible(true);
                String fieldName = declaredField.getName();
                if (isExists) {
                    AtlasProperty property = declaredField.getAnnotation(AtlasProperty.class);
                    if (keys.contains(property.name())) {
                        Iterator<VertexProperty<Object>> properties = vertex.properties(property.name());
                        switch (property.cardinality()) {
                            case LIST:
                            case SET: {
                                ArrayList<Object> objects = new ArrayList<>();
                                while (properties.hasNext()) {
                                    VertexProperty<Object> vertexProperty = properties.next();
                                    objects.add(vertexProperty.value());
                                }
                                declaredField.set(targetObject, objects);
                                break;
                            }
                            case SINGLE: {
                                Object fieldValue = vertex.property(property.name()).value();
                                declaredField.set(targetObject, fieldValue);
                            }
                        }
                    }
                } else {
                    String simpleTypeName = declaredField.getType().getSimpleName();
                    if (!keys.contains(fieldName)) {
                        continue;
                    }
                    if (simpleTypeName.equalsIgnoreCase("list")) {
                        Iterator<VertexProperty<Object>> properties = vertex.properties(fieldName);
                        List<Object> list = new ArrayList<>();
                        while (properties.hasNext()) {
                            VertexProperty<Object> property = properties.next();
                            list.add(property.value());
                        }
                        declaredField.set(targetObject, list);
                    } else {
                        declaredField.set(targetObject, vertex.property(fieldName).value());
                    }
                }
            }
        }
        traversal.tx().commit();
        return targetObject;
    }

    @SneakyThrows
    public List<T> queryByAttr(String attrName, Object value, Long limit, Long offset) {

        AtlasVertex atlasVertex = (AtlasVertex) targetClass.getAnnotation(AtlasVertex.class);
        GraphTraversalSource g = janusGraph.traversal();
        ArrayList<T> list = new ArrayList<>();
        List<org.apache.tinkerpop.gremlin.structure.Vertex> vertices = g.V().hasLabel(atlasVertex.label()).has(attrName, value).skip(offset).limit(limit).toList();
        for (org.apache.tinkerpop.gremlin.structure.Vertex vertex : vertices) {
            T targetObject = (T) targetClass.newInstance();
            Set<String> keys = vertex.keys();
            Field[] declaredFields = targetClass.getDeclaredFields();
            for (Field declaredField : declaredFields) {
                boolean isExists = declaredField.isAnnotationPresent(AtlasProperty.class);
                String fieldName = declaredField.getName();
                declaredField.setAccessible(true);
                if (isExists) {
                    AtlasProperty property = declaredField.getAnnotation(AtlasProperty.class);
                    if (keys.contains(property.name())) {
                        Iterator<VertexProperty<Object>> properties = vertex.properties(property.name());
                        switch (property.cardinality()) {
                            case LIST:
                            case SET: {
                                ArrayList<Object> objects = new ArrayList<>();
                                while (properties.hasNext()) {
                                    VertexProperty<Object> vertexProperty = properties.next();
                                    objects.add(vertexProperty.value());
                                }
                                declaredField.set(targetObject, objects);
                                break;
                            }
                            case SINGLE: {
                                Object fieldValue = vertex.property(property.name()).value();
                                declaredField.set(targetObject, fieldValue);
                            }
                        }
                    }
                } else {
                    String simpleTypeName = declaredField.getType().getSimpleName();
                    if (!keys.contains(fieldName)) {
                        continue;
                    }
                    if (simpleTypeName.equalsIgnoreCase("list")) {
                        Iterator<VertexProperty<Object>> properties = vertex.properties(fieldName);
                        List<Object> vlist = new ArrayList<>();
                        while (properties.hasNext()) {
                            VertexProperty<Object> property = properties.next();
                            vlist.add(property.value());
                        }

                        declaredField.set(targetObject, vlist);
                    } else {
                        declaredField.set(targetObject, vertex.property(fieldName).value());
                    }
                }
            }
            list.add(targetObject);
        }
        return list;
    }

    @SneakyThrows
    public void addRelationship(Object from,Object to,String edgeLabel,Boolean force){
        if(!from.getClass().isAnnotationPresent(AtlasVertex.class) || to.getClass().isAnnotationPresent(AtlasVertex.class)){
            throw new RuntimeException(String.format("实体%s必须指定%s注解", targetClass.getSimpleName(), "@AtlasVertex"));
        }
        AtlasVertex fromVertex = from.getClass().getAnnotation(AtlasVertex.class);
        AtlasVertex toVertex = to.getClass().getAnnotation(AtlasVertex.class);
        //获取from id和to id
        String fromIDProperty = fromVertex.primaryKey();
        String toIDProperty = toVertex.primaryKey();
        Field formIDField = from.getClass().getDeclaredField(fromIDProperty);
        formIDField.setAccessible(true);
        Field toIDField = to.getClass().getDeclaredField(toIDProperty);
        toIDField.setAccessible(true);
        Object fromIDValue = formIDField.get(from);
        Object toIDValue = toIDField.get(to);
        String fromLabel = fromVertex.label();
        String toLabel = toVertex.label();
        GraphTraversalSource g = janusGraph.traversal();
        //删除所有的已经存在的边信息
        Optional<Edge> edgeOption = g.V().has("id", fromIDValue).outE(edgeLabel)
                .has("id", String.format("%s->%s@%s", fromIDValue, toIDValue, edgeLabel)).tryNext();
        if(force){
            log.info("删除{}关系成功！",String.format("%s->%s@%s", fromIDValue, toIDValue, edgeLabel));
            g.V().has("id", fromIDValue).outE(edgeLabel)
                    .has("id", String.format("%s->%s@%s", fromIDValue, toIDValue, edgeLabel)).drop().iterate();
        }
        //如果边关系已经存在则直接更新时间
        if(edgeOption.isPresent() && !force){
            log.info("关系{}已经存在，直接更新一下创建时间！",String.format("%s->%s@%s",fromIDValue,toIDValue,edgeLabel));
            edgeOption.get().property("createTime",new Date());
        }else{
            //更新边信息
            List<Vertex> fromQueryVertex = g.V().hasLabel(fromLabel).has("id", fromIDValue).toList();
            GraphTraversal<Vertex, Vertex> toQueryVertex = g.V().hasLabel(toLabel).has("id", toIDValue);
            g.V(fromQueryVertex).addE(edgeLabel).to(toQueryVertex)
                    .property("from",fromIDValue)
                    .property("to",toIDValue)
                    .property("id",String.format("%s->%s@%s",fromIDValue,toIDValue,edgeLabel))
                    .property("createTime",new Date()).iterate();
            log.info("创建{}边关系成功！",String.format("%s->%s@%s",fromIDValue,toIDValue,edgeLabel));

        }
        g.tx().commit();
    }


    public void clearRelationShip(String edgeLabel){

    }

}
