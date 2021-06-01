package com.jd.janus.store;

import org.janusgraph.core.JanusGraph;

import java.util.List;
import java.util.Map;

public abstract  class GraphStore<T> {
    private JanusGraph janusGraph;


    public abstract T insert(Class< ? extends  T> tarrgetClass, Map<String,Object> attributes);
    public abstract T insert(T t);

    public abstract T update(Class< ? extends  T> tarrgetClass,String attrName,Object value,Map<String,Object> attributes);
    public abstract T update(T t);

    public abstract T upsert(Class< ? extends  T> tarrgetClass,String attrName,Object value,Map<String,Object> attributes);
    public abstract T upsert(T t);

    public abstract Boolean exits(Class< ? extends  T> tarrgetClass,String attrName,Object value);

    public abstract T queryByUniqueAttr(Class< ? extends  T> tarrgetClass,String attrName,Object value);
    public abstract List<T> queryByAttr(Class< ? extends  T> tarrgetClass, String attrName, Object value);


    public void close(){
        janusGraph.close();
    }
}
