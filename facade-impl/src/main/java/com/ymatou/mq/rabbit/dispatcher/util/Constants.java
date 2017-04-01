/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.util;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.ConfigurableApplicationContext;

import com.ymatou.mq.rabbit.dispatcher.config.TomcatConfig;

/**
 * @author luoshiqian 2016/5/11 11:47
 */
public class Constants {

    public static final String APP_ID = "userquery.trading.iapi.ymatou.com";

    public static final String LOG_PREFIX = "logPrefix";

    public static ConfigurableApplicationContext ctx;

    public static TomcatConfig TOMCAT_CONFIG;
    public static final DateTimeFormatter FORMATTER_YYYYMMDDHHMMSS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormat.forPattern("yyyy-MM-dd");


}
