package com.ymatou.mq.rabbit.dispatcher.facade.model;


/**
 * 业务异常
 * 
 * @author tuwenjie
 *
 */
public class BizException extends RuntimeException {
    private static final long serialVersionUID = 1857440708804128584L;
    private ErrorCode errorCode;

    public BizException(ErrorCode errorCode, String msg) {

        this(errorCode, msg, null);
    }

    public BizException(String msg) {
        this(ErrorCode.FAIL, msg);
    }

    public BizException(String msg, Throwable cause) {
        this(ErrorCode.FAIL, msg, cause);
    }

    public BizException(ErrorCode errorCode, String msg, Throwable cause) {

        super(msg, cause);
        if (errorCode == null) {
            throw new IllegalArgumentException("errorCode is null");
        }
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
