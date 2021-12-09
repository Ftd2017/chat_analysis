package com.closer.prophet.validator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Validator {
    public static void main(String[] args) throws IOException {
        String esData = FileUtils.readFileToString(new File("/Users/alextinng/Desktop/ids_23.txt"));
        List<String> odpsData = FileUtils.readLines(new File("/Users/alextinng/Desktop/ids_23.csv"));
        odpsData.remove("\"request_id\"");

        final List<String> odpsIds = new ArrayList<>();

        odpsData.forEach(id -> {
            odpsIds.add(id.replace("\"", ""));
        });


        ObjectMapper mapper = new ObjectMapper();
        Map esMap = mapper.readValue(esData, Map.class);

        Object value = esMap.get("hits");
        LinkedHashMap hits = (LinkedHashMap) value;
        ArrayList list =  (ArrayList) hits.get("hits");

        List<String> esIds = new ArrayList<>();
        for (Object o : list) {
            Map source = (Map) ((LinkedHashMap) o).get("_source");
            esIds.add((String) source.get("request_id"));
        }


        StringBuilder builder = new StringBuilder();
        for(String str : esIds) {
            // 如果不存在返回 -1。
            if(builder.indexOf(","+str+",") > -1) {
                System.out.println("重复的有："+str);
            } else {
                builder.append(",").append(str).append(",");
            }
        }

        Set distinctIds = new HashSet();
        esIds.forEach(distinctIds::add);

        esIds.removeAll(odpsIds);
        System.out.println(esIds.toString());
    }
}
