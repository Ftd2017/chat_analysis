package com.closer.prophet.filter;

import java.io.Serializable;

public class SystemLogFilter implements AbstractFilter, Serializable {
    @Override
    public boolean filter(String log) {
        if (log.contains("\"command\":\"system.error\"") ||
                log.contains("\"command\":\"system.warn\"") ||
                log.contains("\"command\":\"system.info\"")) {
            return false;
        } else {
            return true;
        }
    }
}
