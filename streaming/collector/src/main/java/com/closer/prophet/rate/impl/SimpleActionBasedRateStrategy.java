package com.closer.prophet.rate.impl;

import com.closer.prophet.entity.Action;
import com.closer.prophet.rate.RateStrategy;

import java.io.Serializable;

public class SimpleActionBasedRateStrategy implements RateStrategy, Serializable {

    @Override
    public int rate(Action action) {
        int rating;

        switch (action) {
            case READ_COMMAND:
                rating = 1;
                break;
            case LIKE_COMMAND:
                rating = 2;
                break;
            case COLLECT_COMMAND:
                rating = 3;
                break;
            case SHARE_COMMAND:
                rating = 4;
                break;
            case SHARE_TO_GROUP_COMMAND:
                rating = 4;
                break;
            case COMMENT_COMMAND:
                rating = 5;
                break;
            default:
                rating = 0;
                break;
        }

        return rating;
    }
}