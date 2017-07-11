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

package com.dangdang.ddframe.job.cloud.scheduler.mesos;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Mesos Slave服务.
 *
 * @author liguangyun
 */
public class MesosSlaveService {
    
    /**
     * 获取所有slaves.
     * 
     * @return jsonArray
     */
    public JsonArray findAllSlaves() {
        MesosEndpointService mesosEndpointService = MesosEndpointService.getInstance();
        Optional<JsonObject> jsonObject = mesosEndpointService.slaves(JsonObject.class);
        if (!jsonObject.isPresent()) {
            return new JsonArray();
        }
        JsonArray originalSlaves = jsonObject.get().getAsJsonArray("slaves");
        JsonArray result = new JsonArray();
        for (JsonElement each : originalSlaves) {
            result.add(buildSlave(each.getAsJsonObject()));
        }
        return result;
    }
    
    /**
     * 获取包含指定roleName的slaves.
     * 
     * @return jsonArray
     */
    public JsonArray findSlavesContainsRole(final String roleName) {
        JsonArray result = new JsonArray();
        for (JsonElement eachSlave : findAllSlaves()) {
            for (JsonElement eachRole : eachSlave.getAsJsonObject().getAsJsonArray("roles")) {
                if (eachRole.getAsJsonObject().get("role_name").getAsString().equalsIgnoreCase(roleName)) {
                    result.add(eachSlave);
                    break;
                }
            }
        }
        return result;
    }
    
    private JsonObject buildSlave(final JsonObject rootObject) {
        JsonObject result = new JsonObject();
        result.addProperty("id", rootObject.get("id").getAsString());
        result.addProperty("hostname", rootObject.get("hostname").getAsString());
        result.addProperty("registered_time", rootObject.get("registered_time").getAsDouble());
        if (rootObject.get("reregistered_time") != null) {
            result.addProperty("reregistered_time", rootObject.get("reregistered_time").getAsDouble());
        }
        result.add("resources", buildSummaryResource(rootObject.getAsJsonObject("resources")));
        result.add("use_resources", buildSummaryResource(rootObject.getAsJsonObject("used_resources")));
        result.add("offered_resources", buildSummaryResource(rootObject.getAsJsonObject("offered_resources")));
        result.add("roles", buildRoles(rootObject));
        return result;
    }
    
    private JsonObject buildSummaryResource(final JsonObject rootObject) {
        JsonObject result = new JsonObject();
        result.addProperty("cpus", rootObject.get("cpus").getAsDouble());
        result.addProperty("mem", rootObject.get("mem").getAsInt());
        result.addProperty("disk", rootObject.get("disk").getAsInt());
        return result;
    }
    
    private JsonArray buildRoles(final JsonObject rootObject) {
        final Map<String, JsonObject> roleMap = new HashMap<>();
        fillReservedResources(roleMap, rootObject.getAsJsonObject("reserved_resources_full"));
        fillUsedResources(roleMap, rootObject.getAsJsonArray("used_resources_full"));
        fillOfferedResources(roleMap, rootObject.getAsJsonArray("offered_resources_full"));
        JsonArray result = new JsonArray();
        for (JsonObject each : roleMap.values()) {
            result.add(each);
        }
        return result;
    }
    
    private void fillReservedResources(final Map<String, JsonObject> roleMap, final JsonObject reservedResourcesFull) {
        if (reservedResourcesFull.isJsonNull()) {
            return;
        }
        for (Map.Entry<String, JsonElement> each : reservedResourcesFull.entrySet()) {
            for (JsonElement eachResource : each.getValue().getAsJsonArray()) {
                filledResource(roleMap, eachResource.getAsJsonObject(), "reserved_resources");
            }
        }
    }
    
    private void fillUsedResources(final Map<String, JsonObject> roleMap, final JsonArray usedResourcesFull) {
        if (usedResourcesFull.isJsonNull()) {
            return;
        }
        for (JsonElement each : usedResourcesFull) {
            filledResource(roleMap, each.getAsJsonObject(), "used_resources");
        }
    }
    
    private void fillOfferedResources(final Map<String, JsonObject> roleMap, final JsonArray offeredResourcesFull) {
        if (offeredResourcesFull.isJsonNull()) {
            return;
        }
        for (JsonElement each : offeredResourcesFull) {
            filledResource(roleMap, each.getAsJsonObject(), "offered_resources");
        }
    }
    
    private void filledResource(final Map<String, JsonObject> roleMap, final JsonObject sourceResource, final String resourceType) {
        JsonObject targetResource = buildResource(sourceResource);
        if (roleMap.get(targetResource.get("role_name").getAsString()) == null) {
            JsonObject role = new JsonObject();
            role.addProperty("role_name", targetResource.get("role_name").getAsString());
            roleMap.put(targetResource.get("role_name").getAsString(), role);
        }
        putJsonObjectIfAbsent(roleMap.get(targetResource.get("role_name").getAsString()), targetResource.get("name").getAsString());
        JsonObject jsonObject = roleMap.get(targetResource.get("role_name").getAsString()).get(targetResource.get("name").getAsString()).getAsJsonObject();
        jsonObject.add(resourceType, targetResource);
        putJsonObjectIfAbsent(jsonObject, "reserved_resources");
        putJsonObjectIfAbsent(jsonObject, "used_resources");
        putJsonObjectIfAbsent(jsonObject, "offered_resources");
    }
    
    private void putJsonObjectIfAbsent(final JsonObject jsonObject, final String elementName) {
        if (jsonObject.get(elementName) == null) {
            jsonObject.add(elementName, new JsonObject());
        }
    }
    
    private JsonObject buildResource(final JsonObject resourceObject) {
        JsonObject result = new JsonObject();
        result.addProperty("name", resourceObject.get("name").getAsString());
        result.addProperty("type", resourceObject.get("type").getAsString());
        result.addProperty("role_name", resourceObject.get("role").getAsString());
        if ("SCALAR".equals(result.get("type").getAsString())) {
            result.addProperty("value", resourceObject.getAsJsonObject("scalar").get("value").getAsDouble());
        } else if ("RANGES".equals(result.get("type").getAsString())) {
            result.addProperty("begin", resourceObject.getAsJsonObject("ranges").getAsJsonArray("range").get(0).getAsJsonObject().get("begin").getAsInt());
            result.addProperty("end", resourceObject.getAsJsonObject("ranges").getAsJsonArray("range").get(0).getAsJsonObject().get("end").getAsInt());
        }
        return result;
    }
}
