package com.ymatou.mq.rabbit.dispatcher.facade.model;



/**
 * 响应基类. <em>其所有子类必须有默认的构造函数</em>
 * 
 * @author tuwenjie
 *
 */
public class BaseResponse extends PrintFriendliness {

    private static final long serialVersionUID = -5719901720924490738L;

    private boolean isSuccess;

    private ErrorCode errorCode;

    private String errorMessage;

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public BaseResponse(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public BaseResponse() {}

    public static BaseResponse newSuccessInstance() {
        BaseResponse result = new BaseResponse();
        result.setSuccess(true);
        return result;
    }

    public static BaseResponse newFailInstance(ErrorCode errorCode) {
        BaseResponse result = new BaseResponse();
        result.setSuccess(false);
        result.setErrorCode(errorCode);
        result.setErrorMessage(errorCode == null ? "" : errorCode.getMessage());
        return result;
    }

}
