package com.jw.bean;

/*输入数据样例类*/
public class ApacheLogEvent {
    private String id;

    private String userId;

    //需要进行转换的操作
    private Long timeStamp;

    private String method;

    private String url;

    public ApacheLogEvent() {}

    public ApacheLogEvent(String id, String userId, Long timeStamp, String method, String url) {
        this.id = id;
        this.userId = userId;
        this.timeStamp = timeStamp;
        this.method = method;
        this.url = url;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
