/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */
package com.ymatou.mq.rabbit.dispatcher.facade.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Date;


/**
 * 自身内容能以可读方式输出
 *
 * @author tuwenjie
 */
public abstract class PrintFriendliness implements Serializable {

    private static final DateTimeFormatter FORMATTER_YYYYMMTDDHHMMSS = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter FORMATTER_YYYYMMDDHHMMSS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final long serialVersionUID = -235822080790001901L;

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":"
                + JSON.toJSONString(this, SerializerFeature.WriteDateUseDateFormat, SerializerFeature.SkipTransientField);
    }

    protected Date convertDotNetDateToDate(String value) {
        Date date = null;
        if (value != null) {
            try {
                date = DateTime.parse(value, FORMATTER_YYYYMMTDDHHMMSS).toDate();
            } catch (Exception e) {
                try {
                    date = DateTime.parse(value, FORMATTER_YYYYMMDDHHMMSS).toDate();
                } catch (Exception ex) {

                }
            }
        }
        return date;
    }
}
