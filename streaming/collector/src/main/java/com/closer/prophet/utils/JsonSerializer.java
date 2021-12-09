package com.closer.prophet.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonSerializer {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    JsonSerializer() {}

    public static String mapToJson(Map map) throws JsonProcessingException {
        return mapper.writeValueAsString(map);
    }

    /**
     * Converting a json string to HashMap<String, String>. If value is a sub-json, it will be converted to a string value.
     * @param json Json converted.
     * @return Key/Value pairs.
     * @throws IOException Exception maybe caught while processing json.
     */
    @SuppressWarnings("unchecked")
    public static Map jsonToMap(String json) throws IOException {
        Map kvPairs = new HashMap<>();

        try {
            // convert JSON string to Map
            Map<Object, Object> map = mapper.readValue(json, Map.class);

            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                Object value = entry.getValue();
                if(value instanceof Map) {
                    kvPairs.put(entry.getKey(), mapper.writeValueAsString(value));
                } else {
                    kvPairs.put(entry.getKey(), entry.getValue());
                }
            }
        } catch (IOException e) {
            logger.error("Failed to interpret json!", e);
        }

        return kvPairs;
    }
}
