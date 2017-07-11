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

package com.dangdang.ddframe.job.cloud.scheduler.util;

import com.sun.jersey.api.client.Client;

/**
 * Http工具类.
 *
 * @author liguangyun
 */
public class HttpUtils {
    
    private static final int DEFAULT_CONN_TIMEOUT = 5 * 1000;
    
    private static final int DEFAULT_SO_TIMEOUT = 5 * 1000;
    
    /**
     * GET调用.
     * 
     * @param url url地址
     * @param connectTimeout 连接超时，单位为毫秒
     * @param socketTimeout 传输超时，单位为毫秒
     * @return 调用结果
     */
    public static String get(final String url, final int connectTimeout, final int socketTimeout) {
        Client client = Client.create();
        client.setConnectTimeout(connectTimeout);
        client.setReadTimeout(socketTimeout);
        return client.resource(url).get(String.class);
    }
    
    /**
     * GET调用.
     * 
     * @param url url地址
     * @return 调用结果
     */
    public static String get(final String url) {
        return get(url, DEFAULT_CONN_TIMEOUT, DEFAULT_SO_TIMEOUT);
    }
}
