package com.closer.prophet.mapper;

import com.closer.prophet.utils.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class RecordMapper implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RecordMapper.class);
    private static final String SEPARATOR = ",";
    private static Map<Object, Object> keyMap = new HashMap<>();

    private static final String FIELDS = "event_source_id,ne_captcha_result,sign_api,version_code,version_name,x_closer_sign,client_real_ip,closer_community_id,closer_community_name,closer_group_id,closer_group_name,group_region_name,command,community_id,community_name,data_valid,closer_commdetailunity_name,get_adcookie_value,get_adid_value,group_id,group_name,hack_client_ip,handle,id,int_type,is_register,log_received_time,long_publish_time,method,register_adid,request_id,request_process_time,response_size,response_status,result,server,share_sto,share_udid,sid,sms_limit,sms_send_result,sql_time,subject_id,subject_title,tags,teamid,_timestamp,uid,ums_log_time,user_agent,user_location,video_style,webhook_process_time,x_closer_adid,x_closer_channel,x_closer_register_adid,x_closer_udid,txt_json,p_dt,p_ht";

    static {
        InputStream ins = RecordMapper.class.getClassLoader().getResourceAsStream("key-map.json");
        BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
        StringBuilder sb = new StringBuilder();

        for(Object line : reader.lines().toArray()) {
            sb.append(line);
        }

        String json = sb.toString();
        try {
            keyMap = JsonSerializer.jsonToMap(json);
        } catch (IOException e) {
            logger.error("Failed to read key map table!", e);
        }
    }

    public Map<String, Object> handler(String log) {
        Map<String, Object> map = new HashMap<>();

        String result = FIELDS;

        try {
            Map<Object, Object> kvPairs = JsonSerializer.jsonToMap(log);

            String[] arr = FIELDS.split(SEPARATOR);
            for(String field : arr) {
                String keyOfLog = (String) keyMap.get(field);

                Object value = kvPairs.get(keyOfLog);
                String newValue = null;
                kvPairs.remove(keyOfLog);

                if(value instanceof String) {
                    newValue = (String) value;
                } else if(value instanceof Integer) {
                    newValue = Integer.toString((int) value);
                } else if(value instanceof Long) {
                    newValue = Long.toString((long) value);
                } else if(value instanceof Double) {
                    newValue = Double.toString((double) value);
                } else if(value instanceof Float) {
                    newValue = Float.toString((float) value);
                } else if(value instanceof Boolean) {
                    newValue = Boolean.toString((boolean) value);
                }

                if(null != newValue) {
                    map.put(field, newValue);
                    result = result.replace(field, newValue);
                }
            }

            String textJson = "";
            textJson = JsonSerializer.mapToJson(kvPairs);
            map.put("txt_json", textJson);
        } catch (IOException e) {
            return map;
        }

        return map;
    }
}
