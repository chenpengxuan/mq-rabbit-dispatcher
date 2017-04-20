package com.ymatou.mq.rabbit.dispatcher.support;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.CallbackMessage;
import com.ymatou.mq.infrastructure.support.enums.CallbackFromEnum;
import org.springframework.beans.BeanUtils;

import java.util.Date;

/**
 * CallbackMessage和CallbackConfig包装
 * Created by zhangzhihua on 2017/4/20.
 */
public class CallbackMessageAndConfig extends  CallbackMessage{

    public CallbackMessageAndConfig(){
    }

    public CallbackConfig getCallbackConfig(){
        CallbackConfig callbackConfig = new CallbackConfig();
        //TODO
        return  callbackConfig;
    }

    public static CallbackMessageAndConfig fromCallbackMessageAndConfig(CallbackMessage callbackMessage,CallbackConfig callbackConfig){
        CallbackMessageAndConfig callbackMessageAndConfig = new CallbackMessageAndConfig();
        BeanUtils.copyProperties(callbackMessage,callbackMessageAndConfig);
        return  callbackMessageAndConfig;
    }



}
