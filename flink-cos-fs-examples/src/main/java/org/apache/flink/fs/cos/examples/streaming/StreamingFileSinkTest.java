package org.apache.flink.fs.cos.examples.streaming;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingFileSinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingFileSinkTest.class);
    private static final String JOB_NAME = "flink-cos-streaming-file-sink-test";

    private static void printUsage() {
        System.out.println("Use --input to specify the input data file, and --output to specify the output file. " +
                "For example: '--output cosn://flink-test-1250000000/streamingFileSinkTest'");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if(null == parameterTool.get("output")) {
            printUsage();
            return;
        }
        String outputPath = parameterTool.getRequired("output");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.forRowFormat(
                new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.create().withRolloverInterval(1000).build())
                .build();
        if (parameterTool.get("input") == null) {
            // Use the mockSource to generate the test data
            System.out.println("The '--input' parameter is not specified. "
                    + "Use the inner MockSource instead to generate test data.");
            env.addSource(new MockSource(1000, 100)).addSink(streamingFileSink);
        } else {
            env.readTextFile(parameterTool.get("input")).addSink(streamingFileSink);
        }

        LOG.info("Begin to execute the job: {}.", JOB_NAME);
        env.execute(JOB_NAME);
    }
}
