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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Mesos Endpoint服务.
 *
 * @author liguangyun
 */
public final class MesosEndpointService {
    
    private static final int DEFAULT_CONN_TIMEOUT = 5 * 1000;
    
    private static final int DEFAULT_SO_TIMEOUT = 5 * 1000;
    
    private static MesosEndpointService instance = new MesosEndpointService();
    
    private String masterUrl;
    
    private MesosEndpointService() { }
    
    public static MesosEndpointService getInstance() {
        return instance;
    }
    
    /**
     * 注册Mesos的Master信息.
     * 
     * @param hostName Master的主机名
     * @param port Master端口
     */
    public synchronized void register(final String hostName, final int port) {
        masterUrl = String.format("http://%s:%d", hostName, port);
    }
    
    /**
     * 注销Mesos的Master信息.
     */
    public synchronized void deregister() {
        masterUrl = null;
    }
    
    /**
     * 服务是否有效.
     * 
     * @return 服务是否有效.
     */
    public boolean isValid() {
        return null != masterUrl;
    }
    
    /**
     * 调用 mesos master slaves endpoint.
     * 
     * @param clazz 类
     * @return endpoint调用结果
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> slaves(final Class<T> clazz) {
        if (null == masterUrl) {
            return Optional.absent();
        }
        String result = get(masterUrl + "/slaves");
        if (clazz != String.class) {
            return Optional.of(new Gson().fromJson(result, clazz));
        }
        return Optional.of((T) result);
    }
    
    /**
     * 调用 mesos master state endpoint.
     * 
     * @param clazz 类
     * @return endpoint调用结果
     */
    public <T> Optional<T> state(final Class<T> clazz) {
        if (null == masterUrl) {
            return Optional.absent();
        }
        return state(masterUrl, clazz);
    }

    /**
     * 调用 mesos state endpoint.
     * 
     * @param hostAddress 格式为：http://hostName:port
     * @param clazz 类
     * @return endpoint调用结果
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> state(final String hostAddress, final Class<T> clazz) {
        Preconditions.checkNotNull(hostAddress);
        String result = get(hostAddress + "/state");
        if (clazz != String.class) {
            return Optional.of(new Gson().fromJson(result, clazz));
        }
        return Optional.of((T) result);
    }
    
    private String get(final String url) {
        Client client = Client.create();
        client.setConnectTimeout(DEFAULT_CONN_TIMEOUT);
        client.setReadTimeout(DEFAULT_SO_TIMEOUT);
        return client.resource(url).get(String.class);
    }
}
