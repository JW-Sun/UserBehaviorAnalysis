package com.jw.app.opd02_txMatchDetect;

import java.util.Objects;

public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public ReceiptEvent() {}

    public ReceiptEvent(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }




    @Override
    public String toString() {
        return "ReceiptEvent{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
