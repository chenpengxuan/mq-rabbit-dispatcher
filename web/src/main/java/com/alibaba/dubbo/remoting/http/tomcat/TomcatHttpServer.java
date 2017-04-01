package com.alibaba.dubbo.remoting.http.tomcat;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.http.HttpHandler;
import com.alibaba.dubbo.remoting.http.servlet.DispatcherServlet;
import com.alibaba.dubbo.remoting.http.servlet.ServletManager;
import com.alibaba.dubbo.remoting.http.support.AbstractHttpServer;

import com.ymatou.mq.rabbit.dispatcher.config.TomcatConfig;
import com.ymatou.mq.rabbit.dispatcher.util.Constants;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

import java.io.File;

/**
 * Created by zhangyifan on 2016/9/23.
 */
public class TomcatHttpServer extends AbstractHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(TomcatHttpServer.class);
    private final Tomcat tomcat;
    private final URL url;

    public TomcatHttpServer(URL url, final HttpHandler handler) {
        super(url, handler);

        TomcatConfig tomcatConfig = Constants.TOMCAT_CONFIG;
        this.url = url;
        DispatcherServlet.addHttpHandler(url.getPort(), handler);
        String baseDir = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
        tomcat = new Tomcat();
        tomcat.setBaseDir(baseDir);
        tomcat.setPort(url.getPort());
        tomcat.getConnector().setProtocol(tomcatConfig.getProtocol());
        tomcat.getConnector().setProperty("maxKeepAliveRequests", "-1");
        tomcat.getConnector().setProperty("maxThreads", tomcatConfig.getMaxThreads());
        tomcat.getConnector().setProperty("maxConnections", tomcatConfig.getMaxConnections());
        tomcat.getConnector().setProperty("URIEncoding", tomcatConfig.getUriEncoding());
        tomcat.getConnector().setProperty("connectionTimeout", tomcatConfig.getConnectionTimeout());
        tomcat.getConnector().setProperty("acceptCount", tomcatConfig.getAcceptCount());

        Context context = tomcat.addContext("/", baseDir);
        Tomcat.addServlet(context, "dispatcher", new DispatcherServlet());
        context.addServletMapping("/*", "dispatcher");
        ServletManager.getInstance().addServletContext(url.getPort(), context.getServletContext());

        logger.info("TomcatHttpServerConfig:\n" + tomcatConfig.toString());

        try {
            tomcat.start();
        } catch (LifecycleException e) {
            throw new IllegalStateException("Failed to start tomcat server at " + url.getAddress(), e);
        }
    }

    public void close() {
        super.close();

        ServletManager.getInstance().removeServletContext(url.getPort());

        try {
            tomcat.stop();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }
}
