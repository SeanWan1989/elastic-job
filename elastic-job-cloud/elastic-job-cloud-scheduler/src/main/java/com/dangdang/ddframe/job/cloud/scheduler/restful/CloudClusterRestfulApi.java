/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.cloud.scheduler.restful;

import com.dangdang.ddframe.job.cloud.scheduler.mesos.MesosSlaveService;
import com.google.common.base.Strings;
import com.google.gson.JsonArray;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * 作业云集群的REST API.
 *
 * @author liguangyun
 */
@Path("/cluster")
public final class CloudClusterRestfulApi {
    
    private final MesosSlaveService mesosSlavesService;
    
    public CloudClusterRestfulApi() {
        mesosSlavesService = new MesosSlaveService();
    }
    
    /**
     * 查找全部集群节点.
     * 
     * @return 集群节点集合
     */
    @GET
    @Path("/nodes")
    @Consumes(MediaType.APPLICATION_JSON)
    public JsonArray findAllNodes(@QueryParam("roleName") final String roleName) {
        if (Strings.isNullOrEmpty(roleName)) {
            return mesosSlavesService.findAllSlaves();
        } else {
            return mesosSlavesService.findSlavesContainsRole(roleName);
        }
    }
}
