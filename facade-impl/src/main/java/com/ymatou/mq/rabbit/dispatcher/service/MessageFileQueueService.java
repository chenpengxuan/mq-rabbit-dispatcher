package com.ymatou.mq.rabbit.dispatcher.service;

import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.CallbackMessage;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.rabbit.dispatcher.config.FileDbConf;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ymatou.mq.infrastructure.filedb.FileDb;
import com.ymatou.mq.infrastructure.filedb.FileDbConfig;
import com.ymatou.mq.infrastructure.filedb.PutExceptionHandler;
import com.ymatou.mq.infrastructure.model.Message;

/**
 * 本地消息文件列表处理service Created by zhangzhihua on 2017/3/24.
 */
@Component
public class MessageFileQueueService implements Function<Pair<String, String>, Boolean>, PutExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessageFileQueueService.class);

    private FileDb fileDb;

    @Autowired
    private FileDbConf fileDbConf;

    @Autowired
    private MessageDispatchService messageDispatchService;

    public FileDb getFileDb() {
        return fileDb;
    }

    public void setFileDb(FileDb fileDb) {
        this.fileDb = fileDb;
    }

    @PostConstruct
    public void init() {
        FileDbConfig fileDbConfig = FileDbConfig.newInstance()
                .setDbName(this.fileDbConf.getMsgDbName())
                .setDbPath(this.fileDbConf.getMsgDbPath())
                .setConsumerThreadNums(this.fileDbConf.getMsgDbConsumerThreadNums())
                .setConsumeDuration(this.fileDbConf.getMsgDbConsumeDuration())
                .setMaxConsumeSizeInDuration(this.fileDbConf.getMsgDbMaxConsumeSizeInDuration())
                .setConsumer(this)
                .setPutExceptionHandler(this);

        fileDb = FileDb.newFileDb(fileDbConfig);
    }


    /**
     * 保存成文件队列
     * 
     * @param message
     */
    public boolean saveMessageToFileDb(Message message) {
        try {
            return fileDb.syncPut(message.getId(), Message.toJsonString(message));
        } catch (Exception e) {
            logger.error("sync put file db error.",e);
            return false;
        }
    }

    /**
     * 消费从文件获取到的数据 入库成功 返回true
     * 
     * @param pair
     * @return
     */
    @Override
    public Boolean apply(Pair<String, String> pair) {
        Message message = Message.fromJson(pair.getValue());
        logger.info("consume  message from fileDb,message:{}.",message);
        //保存消息到mongo并直接分发消息
        return messageDispatchService.saveAndDispatchMessage(message);
    }

    @Override
    public void handleException(String key, String value, Optional<Throwable> throwable) {
        //同步保存 不用处理
    }

    @PreDestroy
    public void destroy(){
        fileDb.close();
    }
}
