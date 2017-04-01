package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.AppConfig;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.dispatcher.config.DispatchConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * message dispatch分发service
 * Created by zhangzhihua on 2017/4/1.
 */
@Component
public class MessageDispatchService {

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private DispatchConfig dispatchConfig;

    @Autowired
    private RabbitConfig rabbitConfig;

    @Autowired
    private DispatchCallbackService dispatchCallbackService;

    @PostConstruct
    public void initConsumer(){
        List<AppConfig> appConfigList = messageConfigService.getAllAppConfig();
        for(AppConfig appConfig:appConfigList){
            String dispatchGroup = appConfig.getDispatchGroup();
            if (dispatchGroup != null && dispatchGroup.contains(dispatchConfig.getGroupId())) {
                for (QueueConfig queueConfig : appConfig.getMessageCfgList()) {
                    MessageConsumer messageConsumer = new MessageConsumer(appConfig.getAppId(),queueConfig.getCode());
                    messageConsumer.setRabbitConfig(rabbitConfig);
                    messageConsumer.setDispatchCallbackService(dispatchCallbackService);
                    //TODO set callback service
                    messageConsumer.start();
                }
            }else{
                //TODO
            }
        }
    }

    /**
     * 由接收站直接调用的分发处理接口
     * @param message
     */
    public void dispatch(Message message){
        //TODO
        //TODO 异步写filedb操作
    }
}
