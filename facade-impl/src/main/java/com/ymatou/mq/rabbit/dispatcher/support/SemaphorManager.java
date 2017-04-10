/**
 * (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *
 * All rights reserved.
 */
package com.ymatou.mq.rabbit.dispatcher.support;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.google.common.collect.Maps;
import com.ymatou.mq.infrastructure.model.CallbackConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * 信号量管理器
 * 
 * @author wangxudong 2016年8月16日 下午7:41:52
 *
 */
public class SemaphorManager {

    private static Logger logger = LoggerFactory.getLogger(SemaphorManager.class);

    /**
     * 信号量列表：key = {consumerId}
     */
    private static Map<String, AdjustableSemaphore> semaphoreMap = Maps.newConcurrentMap();

    /**
     * 初始化信号量
     */
    public static void initSemaphores(List<CallbackConfig> callbackConfigList) {
        if(CollectionUtils.isEmpty(callbackConfigList)){
            logger.error("init semaphores error,callbackConfigList is null.");
            return;
        }
        for(CallbackConfig callbackConfig:callbackConfigList){
            initSemaphore(callbackConfig);
        }
        logger.info("initSemaphores success");
    }

    /**
     * 初始化信号量
     *
     * @param callbackConfig
     */
    private static void initSemaphore(CallbackConfig callbackConfig) {
        String consumerId = callbackConfig.getCallbackKey();

        // 防止sit uat数据被改 ，没有callbackkey
        if (StringUtils.isBlank(consumerId)) {
            return;
        }
        int parallelismNum =
                (callbackConfig.getParallelismNum() == null || callbackConfig.getParallelismNum() < 1)
                        ? 2 : callbackConfig.getParallelismNum();
        AdjustableSemaphore semaphore = SemaphorManager.get(consumerId);
        if (semaphore == null) {
            semaphore = new AdjustableSemaphore(parallelismNum);
            SemaphorManager.put(consumerId, semaphore);
        } else {
            semaphore.setMaxPermits(parallelismNum);
        }
    }

    /**
     * 获取信号量
     * 
     * @param key
     * @return
     */
    public static AdjustableSemaphore get(String key) {
        return semaphoreMap.get(key);
    }

    /**
     * 添加信号量
     * 
     * @param key
     * @param semaphore
     */
    public static void put(String key, AdjustableSemaphore semaphore) {
        semaphoreMap.put(key, semaphore);
    }

    /**
     * 获取信号量列表
     * 
     * @return
     */
    public static Map<String, AdjustableSemaphore> getSemaphoreMap() {
        return semaphoreMap;
    }
}
