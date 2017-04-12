/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */
package com.ymatou.mq.rabbit.dispatcher.facade.model;

/**
 * @author tuwenjie
 */
public enum ErrorCode {

    // 请求参数非法
    ILLEGAL_ARGUMENT(100, "请求参数非法"),

    // 明确知道失败原因，但客户端不关心，统一返回请求处理失败
    FAIL(198, "请求处理失败"),

    UNKNOWN(199, "未知错误，系统异常"),

    // 请求处理过程中，出现未知错误
    QUEUE_CONFIG_NOT_EXIST(1001,"队列配置不存在.");

    private int code;

    private String message;

    private ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * 通过错误码获取枚举项
     *
     * @param code
     * @return
     */
    public static ErrorCode getByCode(int code) {
        for (ErrorCode errorCode : ErrorCode.values()) {
            if (errorCode.getCode() == code) {
                return errorCode;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
