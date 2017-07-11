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

import com.dangdang.ddframe.job.cloud.scheduler.restful.AbstractCloudRestfulApiTest;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class MesosSlaveServiceTest extends AbstractCloudRestfulApiTest {
    
    @Test
    public void assertFindAllSlaves() throws Exception {
        MesosSlaveService service = new MesosSlaveService();
        JsonArray slaves = service.findAllSlaves();
        assertThat(slaves.size(), is(1));
        assertThat(slaves.get(0).getAsJsonObject().get("hostname").getAsString(), is("10.10.10.1"));
        assertThat(slaves.get(0).getAsJsonObject().getAsJsonArray("roles").size(), is(2));
        JsonObject role0 = slaves.get(0).getAsJsonObject().getAsJsonArray("roles").get(0).getAsJsonObject();
        JsonObject role1 = slaves.get(0).getAsJsonObject().getAsJsonArray("roles").get(1).getAsJsonObject();
        JsonObject testRole = null;
        JsonObject defaultRole = null;
        if (role0.get("role_name").getAsString().equals("test1")) {
            testRole = role0;
            defaultRole = role1;
        } else {
            testRole = role1;
            defaultRole = role0;
        }
        assertThat(testRole.get("role_name").getAsString(), is("test1"));
        assertThat(testRole.getAsJsonObject("cpus").getAsJsonObject("reserved_resources").get("value").getAsInt(), is(6));
        assertThat(testRole.getAsJsonObject("cpus").getAsJsonObject("used_resources").get("value").getAsDouble(), is(1.014));
        assertThat(testRole.getAsJsonObject("cpus").getAsJsonObject("offered_resources").get("value").getAsDouble(), is(4.986));
        assertThat(testRole.getAsJsonObject("mem").getAsJsonObject("reserved_resources").get("value").getAsInt(), is(6144));
        assertThat(testRole.getAsJsonObject("mem").getAsJsonObject("used_resources").get("value").getAsInt(), is(170));
        assertThat(testRole.getAsJsonObject("mem").getAsJsonObject("offered_resources").get("value").getAsInt(), is(5974));
        assertThat(defaultRole.get("role_name").getAsString(), is("*"));
        assertThat(defaultRole.getAsJsonObject("ports").getAsJsonObject("offered_resources").get("begin").getAsInt(), is(31000));
        assertThat(defaultRole.getAsJsonObject("ports").getAsJsonObject("offered_resources").get("end").getAsInt(), is(32000));
        assertThat(defaultRole.getAsJsonObject("ports").getAsJsonObject("reserved_resources").toString(), is("{}"));
        assertThat(defaultRole.getAsJsonObject("ports").getAsJsonObject("used_resources").toString(), is("{}"));
        assertThat(defaultRole.getAsJsonObject("disk").getAsJsonObject("offered_resources").get("value").getAsInt(), is(41541));
        assertThat(defaultRole.getAsJsonObject("disk").getAsJsonObject("reserved_resources").toString(), is("{}"));
        assertThat(defaultRole.getAsJsonObject("disk").getAsJsonObject("used_resources").toString(), is("{}"));
    }
    
    @Test
    public void assertFindSlavesContainsRole() throws Exception {
        MesosSlaveService service = new MesosSlaveService();
        assertThat(service.findSlavesContainsRole("test1").size(), is(1));
        assertThat(service.findSlavesContainsRole("notExistRole").size(), is(0));
    }
}
