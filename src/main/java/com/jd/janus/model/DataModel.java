package com.jd.janus.model;

import com.jd.janus.annotation.DataType;
import com.jd.janus.annotation.Graph;
import com.jd.janus.annotation.Property;
import com.jd.janus.annotation.Schema;
import org.janusgraph.core.Cardinality;

@Graph(label = "DATA_MODEL",schema = @Schema(properties = {
        @Property(name = "id",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "alias",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "database",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "cluster",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "leadingCadre",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "personInCharge",type = DataType.STRING,cardinality = Cardinality.SET),
        @Property(name = "usageDescription",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "businessDescription",type = DataType.STRING,cardinality = Cardinality.SINGLE),
        @Property(name = "url",type = DataType.STRING,cardinality = Cardinality.SINGLE),
}))
public class DataModel implements VertexModel {

}
