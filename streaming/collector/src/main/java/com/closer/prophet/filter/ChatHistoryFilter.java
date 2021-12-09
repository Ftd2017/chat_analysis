package com.closer.prophet.filter;

import java.io.Serializable;

public class ChatHistoryFilter implements AbstractFilter, Serializable {
    @Override
    public boolean filter(String object) {
        boolean result = false;

        if(org.apache.commons.lang3.StringUtils.isBlank(object)) {
            return false;
        }

        while(object.contains(" :")) {
            object = object.replace(" :", ":");
        }

        // when you have a chat with community secretary, a log without field "communityid",
        // "groupid" will be logged, but it should be removed.
        if (object.toLowerCase().contains("\"communityid\":") &&
                object.contains("\"groupid\":")) {
            result = true;
        }

        return result;
    }
}
