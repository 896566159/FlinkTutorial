package com.xuemi.chapter05;

import java.sql.Timestamp;

public class Event {

    public String user;
    public String url;
    public Long timeStamp;

    public Event() {
    }

    public Event(String user, String url, Long timeStamp) {
        this.user = user;
        this.url = url;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timeStamp=" + new Timestamp(timeStamp) +
                '}';
    }
}
