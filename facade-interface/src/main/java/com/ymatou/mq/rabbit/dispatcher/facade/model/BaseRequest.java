package com.ymatou.mq.rabbit.dispatcher.facade.model;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Set;

/**
 * 请求基类
 * @author wangxudong
 *
 */
public class BaseRequest extends PrintFriendliness {

	/**
	 * 序列化版本
	 */
	private static final long serialVersionUID = 1995776180594622716L;
	
	/**
	 * 数据验证器
	 */
	private static Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

	/**
	 * 应用id
	 */
	private String appId;

	/**
	 * 请求Id
	 */
	private String requestId;

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	/**
	 * 一般请求，requestId不强制必填
	 * @return
	 */
	public boolean requireRequestId() {
		return false;
	}

	/**
	 * 老的.net系统的请求都沒有appId
	 * @return
	 */
	public boolean requireAppId() {
		return false;
	}

	/**
	 * 验证数据有效性
	 */
	public void validate(){
		StringBuilder errorMsgs = new StringBuilder();
		Set<ConstraintViolation<BaseRequest>> violations = VALIDATOR.validate(this);
		
		if(violations != null && violations.size() > 0){
			for (ConstraintViolation<BaseRequest> violation : violations) {
				errorMsgs.append(violation.getMessage()).append("|");
			}
			throw new IllegalArgumentException(errorMsgs.substring(0, errorMsgs.length() - 1));
		}
	}
}
