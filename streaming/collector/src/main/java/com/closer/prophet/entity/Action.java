package com.closer.prophet.entity;

public enum Action {
    COMMENT_COMMAND("closer_reply.add_reply"),
    SHARE_TO_GROUP_COMMAND("closer_subject.share"),
    SHARE_COMMAND("closer_subject.share_subject"),
    COLLECT_COMMAND("closer_subject.collect"),
    LIKE_COMMAND("closer_subject.like"),
    READ_COMMAND("closer_subject.show");

    private String description;

    Action(String description) {
        this.description = description;
    }

    public static Action parseFrom(String description) {
        switch (description) {
            case "closer_reply.add_reply":
                return Action.COMMENT_COMMAND;
            case "closer_subject.share":
                return Action.SHARE_TO_GROUP_COMMAND;
            case "closer_subject.share_subject":
                return Action.SHARE_COMMAND;
            case "closer_subject.collect":
                return Action.COLLECT_COMMAND;
            case "closer_subject.like":
                return Action.LIKE_COMMAND;
            case "closer_subject.show":
                return Action.READ_COMMAND;
            default:
                return null;
        }
    }

    public String getDescription() {
        return description;
    }
}