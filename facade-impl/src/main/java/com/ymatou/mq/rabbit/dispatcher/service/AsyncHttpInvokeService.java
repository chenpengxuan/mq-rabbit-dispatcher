package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.Message;
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

/**
 * async http调用service
 * Created by zhangzhihua on 2017/4/1.
 */
public class AsyncHttpInvokeService implements FutureCallback<HttpResponse> {

    private CloseableHttpAsyncClient httpAsyncClient;

    private Message message;

    private CallbackConfig callbackConfig;

    private DispatchCallbackService dispatchCallbackService;

    public AsyncHttpInvokeService(Message message,CallbackConfig callbackConfig,DispatchCallbackService dispatchCallbackService){
        this.message = message;
        this.callbackConfig = callbackConfig;
        this.dispatchCallbackService = dispatchCallbackService;
        //TODO 创建数量关系
        if(httpAsyncClient == null){
            initAsyncHttpClient();
        }
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

            CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(defaultRequestConfig)
                    .setConnectionManager(cm).build();
            httpClient.start();
        } catch (IOReactorException e) {
            throw new RuntimeException("crate async http client error.",e);
        }
    }

    /**
     * async send
     */
    public void send(){
        send(null);
    }

    /**
     * async send
     * @param timeout
     */
    public void send(Long timeout){
        StringEntity postEntity = new StringEntity(message.getBody(), "UTF-8");
        HttpPost httpPost = new HttpPost(callbackConfig.getUrl());
        httpPost.setEntity(postEntity);
        httpAsyncClient.execute(httpPost,this);
    }

    @Override
    public void completed(HttpResponse result) {
        dispatchCallbackService.onInvokeSuccess(message,callbackConfig);
    }

    @Override
    public void cancelled() {
        dispatchCallbackService.onInvokeFail(message,callbackConfig);
    }

    @Override
    public void failed(Exception ex) {
        dispatchCallbackService.onInvokeFail(message,callbackConfig);
    }


}
