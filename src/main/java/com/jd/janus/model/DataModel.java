package com.jd.janus.model;

import com.jd.janus.annotation.DataType;
import com.jd.janus.annotation.Graph;
import com.jd.janus.annotation.Property;
import lombok.Data;
import org.janusgraph.core.Cardinality;

@Graph(label = "DATA_MODEL")
@Data
public class DataModel implements VertexModel {
   @Property(name = "id",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String id;
    @Property(name = "alias",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String name;
    @Property(name = "database",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String alias;
    @Property(name = "cluster",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String database;
    @Property(name = "leadingCadre",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String cluster;
    @Property(name = "personInCharge",dataType = DataType.STRING,cardinality = Cardinality.SET)
   private String leadingCadre;
    @Property(name = "usageDescription",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String usageDescription;
    @Property(name = "businessDescription",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String businessDescription;
    @Property(name = "url",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
    private String url;
}
