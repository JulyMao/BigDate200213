package com.me;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author maow
 * @create 2020-05-05 10:34
 */
public class CustomInterceptor implements Interceptor {
    public void initialize() {

    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String s = new String(body);
        char c = s.charAt(0);
        if ((c <= 'z' && c >= 'a') || (c <= 'Z' && c >= 'A')) {
            //是字母
            headers.put("type", "alphabet");
        } else {
            //不是字母
            headers.put("type", "number");
        }
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    public void close() {

    }


    public static class MyBuild implements Interceptor.Builder{

        public Interceptor build() {
            return new CustomInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
