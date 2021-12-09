package com.closer.prophet.entity;

public enum SubjectType {

    /**
     * article
     */
    ARTICLE(1)
    /**
     * horizontal video
     */
    ,HORIZONTAL_VIDEO(2)
    /**
     * vertical video
     */
    ,VERTICAL_VIDEO(3)

    /**
     * unknown
     */
    ,UNKNOWN(0);


    private final int value;

    private SubjectType(int value) {
        this.value = value;
    }


    public int getValue(){
        return  value;
    }
}
