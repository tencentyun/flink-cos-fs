/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.cos.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.fs.cos.common.writer.COSAccessHelper;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.NativeFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** The base class for file system factories that create COS file systems. */
public abstract class AbstractCOSFileSystemFactory implements FileSystemFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCOSFileSystemFactory.class);

    private static final ConfigOption<Long> UPLOAD_PART_MIN_SIZE =
            ConfigOptions.key("cos.upload.part.min.size")
                    .defaultValue(FlinkCOSFileSystem.COS_MULTIPART_UPLOAD_PART_MIN_SIZE)
                    .withDescription(
                            ""
                                    + "This option is relevant to the Recoverable Writer and sets the min size of data that "
                                    + "buffered locally before being sent to the COS. This value is limited to the range: 1MB to 5GB.");

    public static final ConfigOption<Integer> MAX_CONCURRENT_UPLOADS =
            ConfigOptions.key("cos.upload.max.concurrent.uploads")
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "This option is relevant to the Recoverable Writer and limits the number of "
                                    + "parts that can be concurrently in-flight. By default, this is set to "
                                    + Runtime.getRuntime().availableProcessors()
                                    + ".");

    private static final ConfigOption<Long> RECOVER_WAIT_TIMESEC =
            ConfigOptions.key("cos.recover.wait.time.seconds")
                    .defaultValue(FlinkCOSFileSystem.COS_RECOVER_WAIT_TIME_SECOND)
                    .withDescription(
                            ""
                                    + "This option is the second wait after recover to make sure the request before recover finish"
                                    + "cos cgi default 60s break the link, it is better to set it bigger than 60");

    // The name of the actual file system.
    private final String name;

    private Configuration flinkConfiguration;

    public AbstractCOSFileSystemFactory(String name) {
        this.name = name;
    }

    @Override
    public void configure(Configuration config) {
        this.flinkConfiguration = config;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        Configuration flinkConfig = this.flinkConfiguration;

        if (flinkConfig == null) {
            LOG.warn(
                    "Creating S3 FileSystem without configuring the factory. All behavior will be default.");
            flinkConfig = new Configuration();
        }

        LOG.info("Creating the COS FileSystem backed by {}.", this.name);
        try {
            org.apache.hadoop.conf.Configuration hadoopConfiguration =
                    this.getHadoopConfiguration();
            org.apache.hadoop.fs.FileSystem fs = createHadoopFileSystem();
            URI uri = getInitURI(fsUri, hadoopConfiguration);
            String bucket = uri.getHost();
            fs.initialize(uri, hadoopConfiguration);

            final String[] localTempDirectories =
                    ConfigurationUtils.parseTempDirectories(flinkConfiguration);
            Preconditions.checkArgument(localTempDirectories.length > 0);
            final String localTempDirectory = localTempDirectories[0];
            final long cosMinPartSize = flinkConfig.getLong(UPLOAD_PART_MIN_SIZE);
            final int maxConcurrentUploads = flinkConfig.getInteger(MAX_CONCURRENT_UPLOADS);
            final long timeoutSec = flinkConfig.getLong(RECOVER_WAIT_TIMESEC);
            final COSAccessHelper cosAccessHelper =
                    getCosAccessHelper(((CosFileSystem)fs).getStore());

            // after hadoop cos fix the setting change to get flag from the cos access helper, avoid head bucket twice.
            // boolean isPosixBucket = cosAccessHelper.isPosixBucket();
            boolean isPosixBucket = ((CosFileSystem)fs).getStore().headBucket(bucket).isMergeBucket();
            boolean isPosixProcess = false;

            // according to the head bucket result and implement config to judge which writer to use.
            String bucketImpl = "";
            if (isPosixBucket) {
                bucketImpl = hadoopConfiguration.get(CosNConfigKeys.COSN_POSIX_BUCKET_FS_IMPL);
                if (null != bucketImpl) {
                    if (bucketImpl.equals(CosNConfigKeys.DEFAULT_COSN_POSIX_BUCKET_FS_IMPL)) {
                        isPosixProcess = true;
                    }
                } else {
                    // default use the posix way to query posix bucket;
                    isPosixProcess = true;
                }
            }

            LOG.info("Creating the Flink cos file system, " +
                    "create posix process recover writer: {}, " +
                            "bucket {} is posix bucket: {}, bucket impl {}.",
                    isPosixProcess, bucket, isPosixBucket, bucketImpl);

            return new FlinkCOSFileSystem(
                    fs,
                    localTempDirectory,
                    cosAccessHelper,
                    cosMinPartSize,
                    maxConcurrentUploads,
                    timeoutSec,
                    isPosixProcess);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    protected abstract org.apache.hadoop.fs.FileSystem createHadoopFileSystem();

    protected abstract URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig);

    protected abstract COSAccessHelper getCosAccessHelper(
            NativeFileSystemStore nativeFileSystemStore);

    protected abstract org.apache.hadoop.conf.Configuration getHadoopConfiguration();
}
