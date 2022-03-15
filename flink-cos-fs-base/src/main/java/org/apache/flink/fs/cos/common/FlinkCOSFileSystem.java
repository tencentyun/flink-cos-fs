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

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.cos.common.utils.RefCountedFile;
import org.apache.flink.fs.cos.common.utils.RefCountedTmpFileCreator;
import org.apache.flink.fs.cos.common.writer.COSAccessHelper;
import org.apache.flink.fs.cos.common.writer.COSRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class FlinkCOSFileSystem extends HadoopFileSystem {

    public static final long COS_MULTIPART_UPLOAD_PART_MIN_SIZE = 1 * 1024 * 1024;

    public static final long COS_RECOVER_WAIT_TIME_SECOND = 30;

    private final String localTmpDir;

    private final FunctionWithException<File, RefCountedFile, IOException> tmpFileCreator;

    private final COSAccessHelper cosAccessHelper;

    private final Executor uploadThreadPool;

    private final long cosUploadPartSize;

    private final int maxConcurrentUploadsPerStream;

    private final long timeoutSec;

    private final long initTimestamp; // second

    /**
     * Wraps the given Hadoop File System object as a Flink File System object. The given Hadoop
     * file system object is expected to be initialized already.
     *
     * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public FlinkCOSFileSystem(
            FileSystem hadoopFileSystem,
            String localTmpDir,
            COSAccessHelper cosAccessHelper,
            long cosUploadPartSize,
            int maxConcurrentUploadsPerStream,
            long timeoutSec) {
        super(hadoopFileSystem);
        this.localTmpDir = Preconditions.checkNotNull(localTmpDir);
        this.tmpFileCreator = RefCountedTmpFileCreator.inDirectories(new File(localTmpDir));
        this.cosAccessHelper = cosAccessHelper;
        this.uploadThreadPool = Executors.newCachedThreadPool();
        this.cosUploadPartSize = cosUploadPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
        this.timeoutSec = timeoutSec;
        this.initTimestamp = System.currentTimeMillis() / 1000; // second
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        if (null == this.cosAccessHelper) {
            throw new UnsupportedOperationException(
                    "This cos file system implementation does not support recoverable writers.");
        }

        return COSRecoverableWriter.writer(
                getHadoopFileSystem(),
                this.tmpFileCreator,
                cosAccessHelper,
                this.uploadThreadPool,
                cosUploadPartSize,
                maxConcurrentUploadsPerStream,
                this.initTimestamp,
                this.timeoutSec);
    }
}
