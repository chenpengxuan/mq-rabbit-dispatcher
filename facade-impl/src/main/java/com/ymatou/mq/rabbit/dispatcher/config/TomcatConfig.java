/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.config;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import com.ymatou.mq.rabbit.dispatcher.facade.model.PrintFriendliness;
import org.springframework.stereotype.Component;

/**
 * Created by zhangyifan on 2016/9/23.
 */
@Component
@DisconfFile(fileName = "tomcat.properties")
public class TomcatConfig extends PrintFriendliness {

    private String protocol;
    private String connectionTimeout;
    private String acceptCount;
    private String maxConnections;
    private String maxThreads;
    private String uriEncoding;

    @DisconfFileItem(name = "protocol")
    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    @DisconfFileItem(name = "connectionTimeout")
    public String getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(String connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    @DisconfFileItem(name = "acceptCount")
    public String getAcceptCount() {
        return acceptCount;
    }

    public void setAcceptCount(String acceptCount) {
        this.acceptCount = acceptCount;
    }

    @DisconfFileItem(name = "maxConnections")
    public String getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(String maxConnections) {
        this.maxConnections = maxConnections;
    }

    @DisconfFileItem(name = "maxThreads")
    public String getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(String maxThreads) {
        this.maxThreads = maxThreads;
    }

    @DisconfFileItem(name = "uriEncoding")
    public String getUriEncoding() {
        return uriEncoding;
    }

    public void setUriEncoding(String uriEncoding) {
        this.uriEncoding = uriEncoding;
    }
}


