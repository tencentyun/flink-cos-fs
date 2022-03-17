package org.apache.flink.fs.cos.examples.streaming;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/** The MockSource for Flink streaming test. */
public class MockSource extends RichParallelSourceFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MockSource.class);
    private volatile boolean runningFlag = false;

    private final long intervalMillis;
    private final int messageNumber;

    public MockSource(long intervalMillis, int count) {
        this.intervalMillis = intervalMillis;
        this.messageNumber = count;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        this.runningFlag = true;
        int messageCount = 1;
        while (this.runningFlag && messageCount < this.messageNumber) {
            String mockMessage =
                    String.format(
                            "Message Number:%d, Time: %s.",
                            messageCount, new Date(System.currentTimeMillis()));
            sourceContext.collect(mockMessage);
            Thread.sleep(this.intervalMillis);
            messageCount++;
        }
    }

    @Override
    public void cancel() {
        this.runningFlag = false;
    }
}
