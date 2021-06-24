package com.jd.janus.model;

import com.jd.janus.annotation.DataType;
import com.jd.janus.annotation.AtlasVertex;
import com.jd.janus.annotation.AtlasProperty;
import lombok.Data;
import lombok.experimental.Accessors;
import org.janusgraph.core.Cardinality;

import java.io.Serializable;
import java.util.List;

@AtlasVertex(label = "DATA_MODEL",primaryKey = "id")
@Data
@Accessors(chain = true)
public class DataModel implements Serializable {
   @AtlasProperty(name = "id",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String id;
    @AtlasProperty(name = "alias",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String alias;
    @AtlasProperty(name = "name",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
    private String name;
    @AtlasProperty(name = "database",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String database;
    @AtlasProperty(name = "cluster",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String cluster;
    @AtlasProperty(name = "leadingCadre",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String leadingCadre;
    @AtlasProperty(name = "personInCharge",dataType = DataType.STRING,cardinality = Cardinality.SET)
   private List<String> personInCharge;
    @AtlasProperty(name = "usageDescription",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String usageDescription;
    @AtlasProperty(name = "businessDescription",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
   private String businessDescription;
    @AtlasProperty(name = "url",dataType = DataType.STRING,cardinality = Cardinality.SINGLE)
    private String url;

}
