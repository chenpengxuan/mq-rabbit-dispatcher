package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.filedb.FileDb;
import com.ymatou.mq.infrastructure.filedb.FileDbConfig;
import com.ymatou.mq.infrastructure.filedb.PutExceptionHandler;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.dispatcher.config.DispatchConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Optional;
import java.util.function.Function;

/**
 * message dispatch分发service
 * Created by zhangzhihua on 2017/4/1.
 */
@Component
public class MessageDispatchService{

    private static final Logger logger = LoggerFactory.getLogger(MessageDispatchService.class);

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private DispatchConfig dispatchConfig;

    @Autowired
    private RabbitConfig rabbitConfig;

    @Autowired
    private DispatchCallbackService dispatchCallbackService;

    @Autowired
    private FileQueueProcessorService fileQueueProcessorService;

    /**
     * 由接收站直接调用的分发处理接口
     * @param message
     */
    public void dispatch(Message message){
        //写fileDb
        boolean result = fileQueueProcessorService.saveMessageToFileDb(message);
        //若写失败，则同步写mongo
        if(!result){

        }
    }


}
