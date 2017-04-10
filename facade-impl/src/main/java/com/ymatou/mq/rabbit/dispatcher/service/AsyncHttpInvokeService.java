package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.dispatcher.support.AdjustableSemaphore;
import com.ymatou.mq.rabbit.dispatcher.support.SemaphorManager;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * async http调用service
 * Created by zhangzhihua on 2017/4/1.
 */
public class AsyncHttpInvokeService implements FutureCallback<HttpResponse> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncHttpInvokeService.class);

    public static final Integer CONN_TIME_OUT = 5000;
    public static final Integer SOCKET_TIME_OUT = 5000;
    public static final Integer CONN_REQ_TIME_OUT = 5000;

    private static RequestConfig DEFAULT_REQUEST_CONFIG = RequestConfig.custom()
            .setConnectTimeout(CONN_TIME_OUT)
            .setSocketTimeout(SOCKET_TIME_OUT)
            .setConnectionRequestTimeout(CONN_REQ_TIME_OUT)
            .build();

    private static  CloseableHttpAsyncClient httpAsyncClient;

    private HttpPost httpPost;

    /**
     * 信号量
     */
    private AdjustableSemaphore semaphore;

    private Message message;

    private CallbackConfig callbackConfig;

    private DispatchCallbackService dispatchCallbackService;

    public AsyncHttpInvokeService(Message message,CallbackConfig callbackConfig,DispatchCallbackService dispatchCallbackService){
        this.message = message;
        this.callbackConfig = callbackConfig;
        this.dispatchCallbackService = dispatchCallbackService;

        if(httpAsyncClient == null){
            initAsyncHttpClient();
        }
        httpPost = new HttpPost(callbackConfig.getUrl());
        this.semaphore = SemaphorManager.get(callbackConfig.getCallbackKey());
        setContentType(callbackConfig.getContentType());
        setTimeout(callbackConfig.getTimeout());
    }

    /**
     * 初始化async http client
     */
    void initAsyncHttpClient(){
        try {
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
            cm.setDefaultMaxPerRoute(20);
            cm.setMaxTotal(100);

            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setSocketTimeout(5000)
                    .setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .build();

            httpAsyncClient = HttpAsyncClients.custom().setDefaultRequestConfig(defaultRequestConfig)
                    .setConnectionManager(cm).build();
            httpAsyncClient.start();
        } catch (IOReactorException e) {
            throw new RuntimeException("crate async http client error.",e);
        }
    }

    /**
     * 设置Content-Type
     *
     * @param contentType
     * @return
     */
    void setContentType(String contentType) {
        if (StringUtils.isEmpty(contentType)) {
            httpPost.setHeader("Content-Type", "application/json;charset=utf-8");
        } else {
            httpPost.setHeader("Content-Type", String.format("%s;charset=utf-8", contentType));
        }
    }

    /**
     * 设置超时
     *
     * @param timeout
     * @return
     */
    void setTimeout(int timeout) {
        RequestConfig requestConfig = RequestConfig.copy(DEFAULT_REQUEST_CONFIG)
                // .setConnectionRequestTimeout(timeout)
                .setSocketTimeout(timeout)
                .build();
        httpPost.setConfig(requestConfig);
    }


    /**
     * async send
     */
    public void send() throws InterruptedException{
        send(null);
    }

    /**
     * async send
     * @param timeout
     */
    public void send(Long timeout) throws InterruptedException{
        if (semaphore != null) {
            if (timeout != null) {
                semaphore.tryAcquire(timeout);
            } else {
                semaphore.acquire();
            }
        }

        StringEntity postEntity = new StringEntity(message.getBody(), "UTF-8");
        httpPost.setEntity(postEntity);
        httpAsyncClient.execute(httpPost,this);
    }

    @Override
    public void completed(HttpResponse result) {
        logger.info("http invoke completed,url:{},result:{}.",callbackConfig.getUrl(),result);
        dispatchCallbackService.onInvokeSuccess(message,callbackConfig,result);
    }

    @Override
    public void cancelled() {
        logger.error("http invoke cancelled,url:{}.",callbackConfig.getUrl());
        dispatchCallbackService.onInvokeFail(message,callbackConfig,null);
    }

    @Override
    public void failed(Exception ex) {
        logger.error("http invoke failed,url:{}.",callbackConfig.getUrl(),ex);
        dispatchCallbackService.onInvokeFail(message,callbackConfig,ex);
    }


}
