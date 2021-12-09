package com.closer.prophet;

import java.util.Locale;

public class StringUtils {
    public static String camelize(String s) {
        StringBuilder sb = new StringBuilder();
        String[] words = s.toLowerCase(Locale.US).split("_");

        for (String word : words)
            sb.append(org.apache.commons.lang.StringUtils.capitalize(word));

        return sb.toString();
    }
}
