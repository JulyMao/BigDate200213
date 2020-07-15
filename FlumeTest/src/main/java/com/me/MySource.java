package com.me;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @author maow
 * @create 2020-05-05 14:54
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;
    private Long time;

    @Override
    public void configure(Context context) {
        prefix = context.getString("pre","maow");
        time = context.getLong("ti",500L);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            // This try clause includes whatever Channel/Event operations you want to do

            // Receive new data
            Event e = getSomeData();

            // Store the Event into this Source's associated Channel(s)
            getChannelProcessor().processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }
        return status;
    }

    private Event getSomeData() throws InterruptedException {
        Event event = new SimpleEvent();

        int i = (int)(Math.random() * 1000);
        String str = prefix + i;
        Thread.sleep(time);
        event.setBody(str.getBytes());
        return event;
    }

    public long getBackOffSleepIncrement() {
        return 1000;
    }

    public long getMaxBackOffSleepInterval() {
        return 10000;
    }
}
