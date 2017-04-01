package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.Message;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

/**
 * async http调用service
 * Created by zhangzhihua on 2017/4/1.
 */
public class AsyncHttpInvokeService implements FutureCallback<HttpResponse> {

    private CloseableHttpAsyncClient httpAsyncClient;

    private Message message;

    private CallbackConfig callbackConfig;

    private DispatchCallbackService dispatchCallbackService;

    public AsyncHttpInvokeService(Message message,CallbackConfig callbackConfig){
        this.message = message;
        this.callbackConfig = callbackConfig;
    }

    /**
     * async send
     */
    public void send(){
        //TODO
    }

    /**
     * async send
     * @param timeout
     */
    public void send(Long timeout){
        StringEntity postEntity = new StringEntity(message.getBody(), "UTF-8");
        HttpPost httpPost = new HttpPost(callbackConfig.getUrl());
        httpPost.setEntity(postEntity);
        //TODO 异常处理
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

    void initAsyncHttpClient(){
//        ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
//        PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
//        cm.setDefaultMaxPerRoute(20);
//        cm.setMaxTotal(100);
//
//        RequestConfig defaultRequestConfig = RequestConfig.custom()
//                .setSocketTimeout(5000)
//                .setConnectTimeout(5000)
//                .setConnectionRequestTimeout(5000)
//                .build();
//
//        HttpAsyncClients httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(defaultRequestConfig)
//                .setConnectionManager(cm).build();
//        httpClient.start();
    }
}
