/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.config;

import com.baidu.disconf.client.common.annotations.DisconfUpdateService;
import com.baidu.disconf.client.common.update.IDisconfUpdate;
import com.ymatou.mq.infrastructure.filedb.FileDbConfig;
import com.ymatou.mq.infrastructure.util.SpringContextHolder;
import com.ymatou.mq.rabbit.dispatcher.service.MessageFileQueueService;
import org.springframework.stereotype.Component;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;

/**
 * @author luoshiqian 2017/3/27 11:25
 */
@Component
@DisconfFile(fileName = "filedb.properties")
@DisconfUpdateService(confFileKeys = "filedb.properties")
public class FileDbConf implements IDisconfUpdate{

    //-----------------msgdb------------------

    /**
     * msgdb store的名称，以及 线程池中的名称
     */
    private String msgDbName;

    /**
     * 文件路径 或 文件夹路径
     */
    private String msgDbPath;
    /**
     * 消费间隔 ms default: 1秒
     */
    private long msgDbConsumeDuration;

    /**
     * 一次消费间隔最大消费数
     */
    private int msgDbMaxConsumeSizeInDuration;

    /**
     * 消费者线程数 默认1
     */
    private int msgDbConsumerThreadNums;

    //-----------------actiondb------------------

    /**
     * action store的名称，以及 线程池中的名称
     */
    private String actionDbName;

    /**
     * 文件路径 或 文件夹路径
     */
    private String actionDbPath;
    /**
     * 消费间隔 ms default: 1秒
     */
    private long actionDbConsumeDuration;

    /**
     * 一次消费间隔最大消费数
     */
    private int actionDbMaxConsumeSizeInDuration;

    /**
     * 消费者线程数 默认1
     */
    private int actionDbConsumerThreadNums;

    @DisconfFileItem(name = "msgdb.dbName")
    public String getMsgDbName() {
        return msgDbName;
    }

    public void setMsgDbName(String msgDbName) {
        this.msgDbName = msgDbName;
    }

    @DisconfFileItem(name = "msgdb.dbPath")
    public String getMsgDbPath() {
        return msgDbPath;
    }

    public void setMsgDbPath(String msgDbPath) {
        this.msgDbPath = msgDbPath;
    }

    @DisconfFileItem(name = "msgdb.consumeDuration")
    public long getMsgDbConsumeDuration() {
        return msgDbConsumeDuration;
    }

    public void setMsgDbConsumeDuration(long msgDbConsumeDuration) {
        this.msgDbConsumeDuration = msgDbConsumeDuration;
    }

    @DisconfFileItem(name = "msgdb.consumerThreadNums")
    public int getMsgDbConsumerThreadNums() {
        return msgDbConsumerThreadNums;
    }

    public void setMsgDbConsumerThreadNums(int msgDbConsumerThreadNums) {
        this.msgDbConsumerThreadNums = msgDbConsumerThreadNums;
    }

    @DisconfFileItem(name = "msgdb.maxConsumeSizeInDuration")
    public int getMsgDbMaxConsumeSizeInDuration() {
        return msgDbMaxConsumeSizeInDuration;
    }

    public void setMsgDbMaxConsumeSizeInDuration(int msgDbMaxConsumeSizeInDuration) {
        this.msgDbMaxConsumeSizeInDuration = msgDbMaxConsumeSizeInDuration;
    }

    @DisconfFileItem(name = "actiondb.dbName")
    public String getActionDbName() {
        return actionDbName;
    }

    public void setActionDbName(String actionDbName) {
        this.actionDbName = actionDbName;
    }

    @DisconfFileItem(name = "actiondb.dbPath")
    public String getActionDbPath() {
        return actionDbPath;
    }

    public void setActionDbPath(String actionDbPath) {
        this.actionDbPath = actionDbPath;
    }

    @DisconfFileItem(name = "actiondb.consumeDuration")
    public long getActionDbConsumeDuration() {
        return actionDbConsumeDuration;
    }

    public void setActionDbConsumeDuration(long actionDbConsumeDuration) {
        this.actionDbConsumeDuration = actionDbConsumeDuration;
    }

    @DisconfFileItem(name = "actiondb.consumerThreadNums")
    public int getActionDbConsumerThreadNums() {
        return actionDbConsumerThreadNums;
    }

    public void setActionDbConsumerThreadNums(int actionDbConsumerThreadNums) {
        this.actionDbConsumerThreadNums = actionDbConsumerThreadNums;
    }

    @DisconfFileItem(name = "actiondb.maxConsumeSizeInDuration")
    public int getActionDbMaxConsumeSizeInDuration() {
        return actionDbMaxConsumeSizeInDuration;
    }

    public void setActionDbMaxConsumeSizeInDuration(int actionDbMaxConsumeSizeInDuration) {
        this.actionDbMaxConsumeSizeInDuration = actionDbMaxConsumeSizeInDuration;
    }

    /**
     * 重新配置线程数，消费间隔，最大消费数量等
     * @throws Exception
     */
    @Override
    public void reload() throws Exception {
        //msgDb
        FileDbConfig msgDbNewConfig = FileDbConfig.newInstance()
                .setConsumerThreadNums(getMsgDbConsumerThreadNums())
                .setConsumeDuration(getMsgDbConsumeDuration())
                .setMaxConsumeSizeInDuration(getMsgDbMaxConsumeSizeInDuration());

        MessageFileQueueService messageFileQueueService = SpringContextHolder.getBean(MessageFileQueueService.class);
        messageFileQueueService.getFileDb().reset(msgDbNewConfig);

        //actionDb
        //FIXME:never used?!!
        FileDbConfig actionDbNewConfig = FileDbConfig.newInstance()
                .setConsumerThreadNums(getActionDbConsumerThreadNums())
                .setConsumeDuration(getActionDbConsumeDuration())
                .setMaxConsumeSizeInDuration(getActionDbMaxConsumeSizeInDuration());
        /*
        MessageFileQueueService messageFileQueueService = SpringContextHolder.getBean(MessageFileQueueService.class);
        messageFileQueueService.getFileDb().reset(msgDbNewConfig);
        */
    }
}
