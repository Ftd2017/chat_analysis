package com.closer.prophet.entity;

import com.closer.prophet.DateFormatUtils;

import java.io.Serializable;
import java.util.Date;

public class ChatHistory implements Serializable {
    private String userId;
    private String groupId;
    private String communityId;
    private String text;
    private Date logReceivedTime;
    private long responseStatus;
    private String sendTo;
    private Date createTime;

    // partition key: day partition and hour partition.
    private String pDt;
    private String pHt;

    public ChatHistory() {
    }

    public ChatHistory(String userId,
                       String groupId,
                       String communityId,
                       String text,
                       Date logReceivedTime,
                       long responseStatus,
                       String sendTo,
                       Date createTime) {
        this.userId = userId;
        this.groupId = groupId;
        this.communityId = communityId;
        this.text = text;
        this.logReceivedTime = logReceivedTime;
        this.responseStatus = responseStatus;
        this.sendTo = sendTo;
        this.createTime = createTime;
        this.pDt = DateFormatUtils.formatDay(logReceivedTime.getTime());
        this.pHt = DateFormatUtils.formatHour(logReceivedTime.getTime());
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getCommunityId() {
        return communityId;
    }

    public void setCommunityId(String communityId) {
        this.communityId = communityId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getSendTo() {
        return sendTo;
    }

    public void setSendTo(String sendTo) {
        this.sendTo = sendTo;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLogReceivedTime() {
        return logReceivedTime;
    }

    public void setLogReceivedTime(Date logReceivedTime) {
        this.logReceivedTime = logReceivedTime;
    }

    public long getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(long responseStatus) {
        this.responseStatus = responseStatus;
    }

    public String getPDt() {
        return pDt;
    }

    public void setPDt(String pDt) {
        this.pDt = pDt;
    }

    public String getPHt() {
        return pHt;
    }

    public void setPHt(String pHt) {
        this.pHt = pHt;
    }
}
