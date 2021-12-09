package com.closer.prophet.utils;

import com.closer.prophet.entity.SubjectType;

import java.util.HashMap;
import java.util.Map;

public class SubjectClassifyUtils {
    private static final Map<Integer, String> SUBJECT_TYPE = new HashMap<>();
    private static final Map<String, String> VIDEO_STYLE = new HashMap<>();

    static {
        SUBJECT_TYPE.put(0, "album");
        SUBJECT_TYPE.put(1, "video");
        SUBJECT_TYPE.put(2, "article");

        VIDEO_STYLE.put("vertical", "vertical video");
        VIDEO_STYLE.put("horizontal", "horizontal video");
    }

    /**
     * Classify subject.
     *
     * @param intType    subject type. 0 - album, 1 - video, 2 - article.
     * @param videoStyle vertical - vertical video, horizontal - horizontal video.
     * @return 0 - unknown, 1 - article, 2 - horizontal video, 3 - vertical video
     */
    public static int classify(int intType, String videoStyle) {
        // return unknown(0) if subject type is empty, or video subject lost video style.
        if (null == SUBJECT_TYPE.get(intType) ||
                (1 == intType && null == VIDEO_STYLE.get(videoStyle))) {
            return SubjectType.UNKNOWN.getValue();
        }

        // album and article are not different.
        if(1 != intType) {
            return SubjectType.ARTICLE.getValue();
        }

        if(videoStyle.equals("horizontal")) {
            return SubjectType.HORIZONTAL_VIDEO.getValue();
        } else {
            return SubjectType.VERTICAL_VIDEO.getValue();
        }
    }
}
