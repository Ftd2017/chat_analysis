package com.closer.prophet.serialization;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import com.closer.prophet.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RecordEntryDeserializer {
    private static final Logger logger = LoggerFactory.getLogger(RecordEntryDeserializer.class);

    RecordEntryDeserializer() {
    }

    /**
     * Deserialize object instance to RecordEntry instance.
     *
     * @param clazz    Class of object instance.
     * @param instances Object instance.
     * @param schema   Schema of record.
     * @return A list of RecordEntry instance.
     */
    public static List<RecordEntry> deserialize(Class clazz, List<?> instances, RecordSchema schema) {
        List<RecordEntry> recordEntries = new ArrayList<>();

        for(Object object : instances) {
            RecordEntry entry = deserialize(clazz, object, schema);
            recordEntries.add(entry);
        }

        return recordEntries;
    }

    /**
     * Deserialize object instance to RecordEntry instance.
     *
     * @param clazz    Class of object instance.
     * @param instance Object instance.
     * @param schema   Schema of record.
     * @return A RecordEntry instance.
     */
    private static RecordEntry deserialize(Class clazz, Object instance, RecordSchema schema) {
        List<Field> fields = schema.getFields();
        RecordEntry entry = new RecordEntry(schema);
        String randomString = RandomStringUtils.random(9);
        entry.setPartitionKey(randomString);

        for (Field field : fields) {
            String fieldName = field.getName();

            String method = "get" + StringUtils.camelize(fieldName);

            try {
                Method getMethod = clazz.getDeclaredMethod(method);
                Object value = getMethod.invoke(instance);

                setValueToEntry(fieldName, value, entry);
            } catch (NoSuchMethodException e) {
                logger.error("Method not found!", e);
            } catch (IllegalAccessException e) {
                logger.error("Can not access method!", e);
            } catch (InvocationTargetException e) {
                logger.error("Catch InvocationTargetException!", e);
            }
        }

        return entry;
    }

    /**
     * Invoke different set method according to object type.
     *
     * @param key   Field name.
     * @param value Value of field.
     * @param entry Destination RecordEntry instance.
     */
    private static void setValueToEntry(String key, Object value, RecordEntry entry) {
        if (value instanceof Integer) {
            entry.setBigint(key, (long) ((Integer) value));
        } else if (value instanceof Long) {
            entry.setBigint(key, (Long) value);
        } else if (value instanceof Float) {
            entry.setDouble(key, (double) ((Float) value));
        } else if (value instanceof Double) {
            entry.setDouble(key, (double) value);
        } else if (value instanceof String) {
            entry.setString(key, (String) value);
        } else if (value instanceof Boolean) {
            entry.setBoolean(key, (Boolean) value);
        } else if (value instanceof Date) {
            entry.setTimeStampInDate(key, (Date) value);
        } else if (value instanceof BigDecimal) {
            entry.setDecimal(key, (BigDecimal) value);
        }
    }
}
