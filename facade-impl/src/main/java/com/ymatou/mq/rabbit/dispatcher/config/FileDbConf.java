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
import com.ymatou.mq.rabbit.dispatcher.service.FileQueueProcessorService;
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

    /**
     * store的名称，以及 线程池中的名称
     */
    private String dbName;

    /**
     * 文件路径 或 文件夹路径
     */
    private String dbPath;
    /**
     * 消费间隔 ms default: 1秒
     */
    private long consumeDuration;

    /**
     * 一次消费间隔最大消费数
     */
    private int maxConsumeSizeInDuration;

    /**
     * 消费者线程数 默认1
     */
    private int consumerThreadNums;


    @DisconfFileItem(name = "filedb.dbName")
    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @DisconfFileItem(name = "filedb.dbPath")
    public String getDbPath() {
        return dbPath;
    }

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    @DisconfFileItem(name = "filedb.consumeDuration")
    public long getConsumeDuration() {
        return consumeDuration;
    }

    public void setConsumeDuration(long consumeDuration) {
        this.consumeDuration = consumeDuration;
    }

    @DisconfFileItem(name = "filedb.maxConsumeSizeInDuration")
    public int getMaxConsumeSizeInDuration() {
        return maxConsumeSizeInDuration;
    }

    public void setMaxConsumeSizeInDuration(int maxConsumeSizeInDuration) {
        this.maxConsumeSizeInDuration = maxConsumeSizeInDuration;
    }

    @DisconfFileItem(name = "filedb.consumerThreadNums")
    public int getConsumerThreadNums() {
        return consumerThreadNums;
    }

    public void setConsumerThreadNums(int consumerThreadNums) {
        this.consumerThreadNums = consumerThreadNums;
    }

    /**
     * 重新配置线程数，消费间隔，最大消费数量等
     * @throws Exception
     */
    @Override
    public void reload() throws Exception {

        FileDbConfig newConfig = FileDbConfig.newInstance()
                .setConsumerThreadNums(getConsumerThreadNums())
                .setConsumeDuration(getConsumeDuration())
                .setMaxConsumeSizeInDuration(getMaxConsumeSizeInDuration());

        FileQueueProcessorService fileQueueProcessorService = SpringContextHolder.getBean(FileQueueProcessorService.class);
        fileQueueProcessorService.getFileDb().reset(newConfig);
    }
}
