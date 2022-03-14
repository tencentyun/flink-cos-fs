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
package org.apache.flink.fs.cos.common.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.cos.common.FlinkCOSFileSystem;
import org.apache.flink.fs.cos.common.utils.RefCountedFile;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class COSRecoverableWriter implements RecoverableWriter {
    private final FunctionWithException<File, RefCountedFile, IOException> tempFileCreator;

    private final long userDefinedMinPartSize;

    private final COSAccessHelper cosAccessHelper;

    private final COSRecoverableMultipartUploadFactory uploadFactory;

    public COSRecoverableWriter(
            final COSAccessHelper cosAccessHelper,
            final COSRecoverableMultipartUploadFactory uploadFactory,
            final FunctionWithException<File, RefCountedFile, IOException> tempFileCreator,
            long userDefinedMinPartSize) {
        this.tempFileCreator = checkNotNull(tempFileCreator);
        this.userDefinedMinPartSize = userDefinedMinPartSize;
        this.cosAccessHelper = checkNotNull(cosAccessHelper);
        this.uploadFactory = checkNotNull(uploadFactory);
    }

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        final RecoverableMultipartUpload upload = uploadFactory.getNewRecoverableUpload(path);

        return COSRecoverableFsDataOutputStream.newStream(
                upload, tempFileCreator, userDefinedMinPartSize);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException {
        final COSRecoverable cosRecoverable = castToCOSRecoverable(resumable);
        final RecoverableMultipartUpload upload =
                uploadFactory.recoverRecoverableUpload(cosRecoverable);
        return COSRecoverableFsDataOutputStream.recoverStream(
                upload,
                tempFileCreator,
                userDefinedMinPartSize,
                cosRecoverable.getNumBytesInParts());
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return true;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        final COSRecoverable cosRecoverable = castToCOSRecoverable(resumable);
        final String smallPartObjectToDelete = cosRecoverable.getInCompleteObjectName();
        return smallPartObjectToDelete != null
                && cosAccessHelper.deleteObject(smallPartObjectToDelete);
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable)
            throws IOException {
        final COSRecoverable cosRecoverable = castToCOSRecoverable(resumable);
        final COSRecoverableFsDataOutputStream recovered =
                (COSRecoverableFsDataOutputStream) recover(cosRecoverable);
        return recovered.closeForCommit();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        return (SimpleVersionedSerializer) COSRecoverableSerializer.INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        return (SimpleVersionedSerializer) COSRecoverableSerializer.INSTANCE;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    private static COSRecoverable castToCOSRecoverable(CommitRecoverable recoverable) {
        if (recoverable instanceof COSRecoverable) {
            return (COSRecoverable) recoverable;
        }
        throw new IllegalArgumentException(
                "COS File System cannot recover recoverable for other file system: " + recoverable);
    }

    public static COSRecoverableWriter writer(
            final FileSystem fs,
            final FunctionWithException<File, RefCountedFile, IOException> tempFileCreator,
            final COSAccessHelper cosAccessHelper,
            final Executor uploadThreadPool,
            final long userDefinedMinPartSize,
            final int maxConcurrentUploadsPerStream) {

        checkArgument(
                userDefinedMinPartSize >= FlinkCOSFileSystem.COS_MULTIPART_UPLOAD_PART_MIN_SIZE);

        final COSRecoverableMultipartUploadFactory uploadFactory =
                new COSRecoverableMultipartUploadFactory(
                        fs,
                        cosAccessHelper,
                        maxConcurrentUploadsPerStream,
                        uploadThreadPool,
                        tempFileCreator);

        return new COSRecoverableWriter(
                cosAccessHelper, uploadFactory, tempFileCreator, userDefinedMinPartSize);
    }
}
