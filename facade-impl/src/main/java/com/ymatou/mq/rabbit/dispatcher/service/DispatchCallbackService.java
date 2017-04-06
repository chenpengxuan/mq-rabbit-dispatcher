package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.rabbit.dispatcher.support.AdjustableSemaphore;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * 消息分发callack service
 * Created by zhangzhihua on 2017/4/1.
 */
@Component
public class DispatchCallbackService {

    private static final Logger logger = LoggerFactory.getLogger(DispatchCallbackService.class);

    private AdjustableSemaphore semaphore;

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private MessageService messageService;

    /**
     * 回调处理
     * @param message
     */
    public void invoke(Message message){
        List<CallbackConfig> callbackConfigList = messageConfigService.getCallbackConfigList(message.getAppId(),message.getQueueCode());
        if(CollectionUtils.isEmpty(callbackConfigList)){
            logger.error("appId:{},queueCode:{} not exist subscribler.",message.getAppId(),message.getQueueCode());
            return;
        }

        for(CallbackConfig callbackConfig:callbackConfigList){
            try {
                doInvokeOne(message,callbackConfig,null);
            } catch (InterruptedException e) {
                //TODO 异常处理&返回值
                logger.error("invoke one error.",e);
            }
        }

    }

    /**
     * 调用一个订阅者
     * @param message
     * @param callbackConfig
     */
    void doInvokeOne(Message message,CallbackConfig callbackConfig,Long timeout) throws InterruptedException {
        //TODO 信号量处理
        if (semaphore != null) {
            if (timeout != null) {
                semaphore.tryAcquire(timeout);
            } else {
                semaphore.acquire();
            }
        }

        //async http send
        new AsyncHttpInvokeService(message,callbackConfig,this).send();
    }

    /**
     *
     * @param message
     * @param callbackConfig
     */
    public void onInvokeSuccess(Message message,CallbackConfig callbackConfig,HttpResponse result){
        //TODO 更新分发明细状态
        messageService.updateMessageStatus(message);
    }

    /**
     *
     * @param message
     * @param callbackConfig
     */
    public void onInvokeFail(Message message,CallbackConfig callbackConfig,Exception ex){
        if(this.isNeedRetry(callbackConfig)){//若需要重试
            //TODO 进行重试操作
        }else{//若不需要重试
            boolean isNeedCompensate = this.isNeedCompensate(callbackConfig);
            if(!isNeedCompensate){//若不需要补单
                //TODO 更新分发明细状态
                messageService.updateMessageStatus(message);
            }else{//若需要补
                //TODO 插补单
                messageService.insertCompensate(message);
                //TODO 更新分发明细状态
                messageService.updateMessageStatus(message);
            }
        }
    }

    /**
     * 判断是否需要重试
     * @param callbackConfig
     * @return
     */
    boolean isNeedRetry(CallbackConfig callbackConfig){
        return false;
    }

    /**
     * 判断是否需要补单
     * @param callbackConfig
     * @return
     */
    boolean isNeedCompensate(CallbackConfig callbackConfig){
        return false;
    }

}
