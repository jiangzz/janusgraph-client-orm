package com.jd.janus.model;

import com.jd.janus.annotation.AtlasVertex;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

@AtlasVertex(label = "JOB",primaryKey = "id")
@Data
@Accessors(chain = true)
public class Job implements Serializable {
   private String id;
   private String type;
    private String name;
   private String alias;
   private Date createTime;
   private Date runDate;
   private String leadingCadre;
   private List<String> personInCharge;
   private String status;
    private String url;
}
