package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.*;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.infrastructure.support.enums.CallbackFromEnum;
import com.ymatou.mq.infrastructure.support.enums.DispatchStatusEnum;
import com.ymatou.mq.rabbit.dispatcher.support.AdjustableSemaphore;
import org.apache.http.HttpResponse;
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
        CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,result);
        messageService.updateDispatchDetail(callbackResult);
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
            if(isNeedCompensate){//若需要补单
                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,ex,true);
                messageService.updateDispatchDetail(callbackResult);
            }else{//若不需要补单
                //TODO 插补单
                MessageCompensate messageCompensate = this.buildCompensate(message,callbackConfig);
                messageService.insertCompensate(messageCompensate);
                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,ex,false);
                messageService.updateDispatchDetail(callbackResult);
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

    /**
     * 构造回调结果
     * @param message
     * @param callbackConfig
     * @param result
     * @return
     */
    CallbackResult buildCallbackResult(Message message,CallbackConfig callbackConfig,HttpResponse result){
        CallbackResult callbackResult = new CallbackResult();
        //调用来源
        callbackResult.setFrom(CallbackFromEnum.DISPATCH.ordinal());
        //TODO 补字段属性
        //调用url
        //调用请求报文
        //调用响应报文
        //调用开始时间
        //调用结束时间
        //调用结果
        if(result != null && result.getStatusLine() != null && result.getStatusLine().getStatusCode() == 200){
            callbackResult.setResult(DispatchStatusEnum.SUCCESS.ordinal());
        }else{
            callbackResult.setResult(DispatchStatusEnum.FAIL.ordinal());
        }
        //计数+1 TODO
        return callbackResult;
    }

    /**
     * 构造回调结果
     * @param message
     * @param callbackConfig
     * @param ex
     * @param isNeedCompensate
     * @return
     */
    CallbackResult buildCallbackResult(Message message,CallbackConfig callbackConfig,Exception ex,boolean isNeedCompensate){
        CallbackResult callbackResult = new CallbackResult();
        //调用来源
        callbackResult.setFrom(CallbackFromEnum.DISPATCH.ordinal());
        //TODO 补字段属性
        //调用url
        //调用请求报文
        //调用响应报文
        //调用开始时间
        //调用结束时间
        //调用结果
        if(isNeedCompensate){
            callbackResult.setResult(DispatchStatusEnum.COMPENSATE.ordinal());
        }else{
            callbackResult.setResult(DispatchStatusEnum.FAIL.ordinal());
        }
        //计数+1 TODO
        return callbackResult;
    }

    /**
     * 构造补单
     * @param message
     * @param callbackConfig
     * @return
     */
    MessageCompensate buildCompensate(Message message,CallbackConfig callbackConfig){
        //TODO
        MessageCompensate messageCompensate = new MessageCompensate();
        return messageCompensate;
    }

}
