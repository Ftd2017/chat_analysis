package com.closer.prophet;

import com.aliyun.datahub.model.RecordEntry;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;

public class OdpsValueSetter {
    OdpsValueSetter() {}

    public static void setBigint(String key, Object value, RecordEntry entry) {
        if (value instanceof Integer) {
            entry.setBigint(key, (long) ((Integer) value));
        } else if (value instanceof Long) {
            entry.setBigint(key, (Long) value);
        }
    }

    public static void setDouble(String key, Object value, RecordEntry entry) {
        if (value instanceof Float) {
            entry.setDouble(key, (double) ((Float) value));
        } else if (value instanceof Double) {
            entry.setDouble(key, (Double) value);
        }
    }

    public static void setBoolean(String key, Object value, RecordEntry entry) {
        if (value instanceof Boolean) {
            entry.setBoolean(key, (Boolean) value);
        }
    }

    public static void setTimestamp(String key, Object value, RecordEntry entry) {
        if (value instanceof Long) {
            entry.setTimeStampInUs(key, (Long) value);
        } else if (value instanceof Date) {
            entry.setTimeStampInDate(key, (Date) value);
        } else if (value instanceof String) {
            Date date = DateTime.parse((String) value).toDate();
            entry.setTimeStampInDate(key, date);
        }
    }

    public static void setDecimal(String key, Object value, RecordEntry entry) {
        if (value instanceof BigDecimal) {
            entry.setDecimal(key, (BigDecimal) value);
        }
    }

    public static void setString(String key, Object value, RecordEntry entry) {
        if (value instanceof String) {
            entry.setString(key, (String) value);
        }
    }
}
