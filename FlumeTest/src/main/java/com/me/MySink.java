package com.me;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.IOException;

/**
 * @author maow
 * @create 2020-05-05 15:16
 */
public class MySink extends AbstractSink implements Configurable {
    private String myProp;

    @Override
    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");

        // Process the myProp value (e.g. validation)

        // Store myProp for later retrieval by process() method
        this.myProp = myProp;
    }


    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do

            Event event = ch.take();

            // Send the Event to the external repository.
            // storeSomeData(e);
            storeSomeData(event);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    private void storeSomeData(Event event) throws IOException {
        byte[] body = event.getBody();
        System.out.write(body);
        System.out.println();

    }
}