package com.closer.prophet.entity;

import com.closer.prophet.DateFormatUtils;

import java.io.Serializable;
import java.util.Date;

public class BrowsingHistory implements Serializable {
    private static final String FROM_SOURCE = "grouk-log";
    private static final String DEFAULT_CREATOR = "SparkStreaming";

    // identifier of article.
    private String articleId;
    // identifier of user who are reading the article {articleId}.
    private String userId;
    // the bigger rate are, the more important the user action is.
    private int rate;
    // time when user read the article.
    private Date readTime;
    // publish time of article
    private Date publishTime;
    // description of article
    private String title;
    // identifier of device.
    private String udid;
    // type of device.
    private String userAgent;
    // type of subject(article, album, video)
    private int intType;
    // type of video(vertical, horizontal)
    // type of subject. 1 - article or album, 1 - horizontal video, 2 - vertical video
    private int type;
    private String videoStyle;
    private Date createTime;
    private String creator;
    private String fromSource;

    // partition key: day partition and hour partition.
    private String pDt;
    private String pHt;

    public BrowsingHistory() {
    }

    public BrowsingHistory(String userId,
                           String articleId,
                           int rate,
                           Date readTime,
                           String title,
                           Date publishTime,
                           String udid,
                           String userAgent,
                           int type) {
        this.userId = userId;
        this.articleId = articleId;
        this.rate = rate;
        this.readTime = readTime;
        this.title = title;
        this.publishTime = publishTime;
        this.udid = udid;
        this.userAgent = userAgent;

        this.pDt = DateFormatUtils.formatDay(readTime.getTime());
        this.pHt = DateFormatUtils.formatHour(readTime.getTime());
        this.type = type;

        this.fromSource = FROM_SOURCE;
        this.createTime = new Date();
        this.creator = DEFAULT_CREATOR;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public Date getReadTime() {
        return readTime;
    }

    public void setReadTime(Date readTime) {
        this.readTime = readTime;
    }

    public Date getPublishTime() {
        return publishTime;
    }

    public void setPublishTime(Date publishTime) {
        this.publishTime = publishTime;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String description) {
        this.title = title;
    }

    public String getUdid() {
        return udid;
    }

    public void setUdid(String udid) {
        this.udid = udid;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public int getIntType() {
        return intType;
    }

    public void setIntType(int intType) {
        this.intType = intType;
    }

    public String getVideoStyle() {
        return videoStyle;
    }

    public void setVideoStyle(String videoStyle) {
        this.videoStyle = videoStyle;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getFromSource() {
        return fromSource;
    }

    public void setFromSource(String fromSource) {
        this.fromSource = fromSource;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
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