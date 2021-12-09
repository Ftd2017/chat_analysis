package com.closer.prophet.classifier;

/**
 * Classifying logs into different types. Default types contains 'Browsing History' and 'Chat History'.
 */
public class DefaultClassifier {
    public static final int BROWSING_HISTORY = 0;
    public static final int CHAT_HISTORY = 1;

    public static int classify(String log) {
        // remove white space from json, cause white space will cause mistake sometimes.
        log = log.replace(" ", "");

        if (log.contains("\"command\":\"message.send\",")) {
            return CHAT_HISTORY;
        } else if (log.contains("\"command\":\"closer_reply.add_reply\",") ||
                log.contains("\"command\":\"closer_subject.show\",") ||
                log.contains("\"command\":\"closer_subject.like\",") ||
                log.contains("\"command\":\"closer_subject.collect\",") ||
                log.contains("\"command\":\"closer_subject.share\",") ||
                log.contains("\"command\":\"closer_subject.share_subject\",")) {
            return BROWSING_HISTORY;
        }

        return -1;
    }
}
