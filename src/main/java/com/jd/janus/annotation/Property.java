package com.jd.janus.annotation;

import org.janusgraph.core.Cardinality;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Property {
    String name();
    DataType dataType();
    Cardinality cardinality();
}
