package com.closer.prophet;

import java.util.Date;

public class DateFormatUtils extends org.apache.commons.lang.time.DateFormatUtils {


    private static final String DEFAULT_DATE_PATTERN = "yyyy-MM";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    public static String formatMonth(Date now) {
        return format(now, DEFAULT_DATE_PATTERN);
    }

    public static String formatDay(long now) {
        return format(now, DATE_FORMAT);
    }

    public static String formatTimestamp(long now) {
        return format(now, TIMESTAMP_FORMAT);
    }

    public static String formatHour(long now) {
        return format(now, HOUR_FORMAT);
    }
}