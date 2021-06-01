package com.jd.janus.annotation;

import org.janusgraph.core.Cardinality;

public @interface Property {
    String name();
    DataType type();
    Cardinality cardinality();
}
