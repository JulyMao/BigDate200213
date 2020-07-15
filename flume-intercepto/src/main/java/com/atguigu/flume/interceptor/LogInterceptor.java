package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 * @author maow
 * @create 2020-05-12 21:49
 */
public class LogInterceptor implements Interceptor {
    public void initialize() {

    }

    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        if (JSON.isValid(log)) {
            return event;
        } else {
            return null;
        }
    }

    public List<Event> intercept(List<Event> list) {

        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) {
            Event next = iterator.next();
            if (intercept(next) == null) {
                iterator.remove();
            }
        }
        return list;

    }

    public void close() {

    }
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
