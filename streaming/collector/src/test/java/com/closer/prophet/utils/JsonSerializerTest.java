package com.closer.prophet.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class JsonSerializerTest {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializerTest.class);

    private String TESTED_JSON;
    private Map<Object, Object> EXPECTED_MAP;
    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp() {
        EXPECTED_MAP = new HashMap<>();

        Map<String, Object> kvPairs = new HashMap<>();
        kvPairs.put("server", "ums_server_1");
        kvPairs.put("-event_source_id", 9821);
        kvPairs.put("response_status", "200");

        Map<String, String> eventSourceInfo = new HashMap<>();
        eventSourceInfo.put("event_id", "oxg0623");
        eventSourceInfo.put("agent", "IOS");

        try {
            EXPECTED_MAP.putAll(kvPairs);
            EXPECTED_MAP.put("event_source_info", mapper.writeValueAsString(eventSourceInfo));
            kvPairs.put("event_source_info", eventSourceInfo);

            TESTED_JSON = mapper.writeValueAsString(kvPairs);

            // change event info to string and put back.
            kvPairs.put("event_source_info", mapper.writeValueAsBytes(eventSourceInfo));

        } catch (JsonProcessingException e) {
            logger.error("Failed to convert map to json!", e);
        }
    }

    @Test
    public void mapToJson() {
    }

    @Test
    public void jsonToMap() throws IOException {
        Map<Object, Object> map = JsonSerializer.jsonToMap(TESTED_JSON);

        String eventSourceInfo = (String) map.get("event_source_info");
        Map<Object, Object> eveSrc = new ObjectMapper().readValue(eventSourceInfo, Map.class);

        Assert.assertEquals("oxg0623", eveSrc.get("event_id"));
        Assert.assertEquals("IOS", eveSrc.get("agent"));
        Assert.assertEquals("ums_server_1", map.get("server"));
        Assert.assertEquals(9821, map.get("-event_source_id"));
        Assert.assertEquals("200", map.get("response_status"));

    }
}