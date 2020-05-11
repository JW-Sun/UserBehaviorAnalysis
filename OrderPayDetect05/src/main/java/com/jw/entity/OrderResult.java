package com.jw.entity;

public class OrderResult {

    private String orderId;
    private String resultMsg;

    public OrderResult() {}

    public OrderResult(String orderId, String resultMsg) {
        this.orderId = orderId;
        this.resultMsg = resultMsg;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId='" + orderId + '\'' +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }
}
