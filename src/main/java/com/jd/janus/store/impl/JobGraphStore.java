package com.jd.janus.store.impl;

import com.jd.janus.configuration.JanusApplicationProperties;
import com.jd.janus.model.DataModel;
import com.jd.janus.model.Job;
import com.jd.janus.store.GraphStore;

public class JobGraphStore extends GraphStore<Job> {
    public JobGraphStore() {
        super(JanusApplicationProperties.get());
    }
}
