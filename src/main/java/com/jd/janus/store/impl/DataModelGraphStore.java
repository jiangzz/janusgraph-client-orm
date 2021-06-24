package com.jd.janus.store.impl;

import com.jd.janus.configuration.JanusApplicationProperties;
import com.jd.janus.model.DataModel;
import com.jd.janus.store.GraphStore;

public class DataModelGraphStore extends GraphStore<DataModel> {
    public DataModelGraphStore() {
        super(JanusApplicationProperties.get());
    }
}
