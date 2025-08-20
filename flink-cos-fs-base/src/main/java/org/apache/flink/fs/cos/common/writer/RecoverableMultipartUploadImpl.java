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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.fs.cos.common.utils.RefCountedFSOutputStream;

import com.qcloud.cos.model.PartETag;
//import com.qcloud.cos.thirdparty.org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The Recoverable MPU implementation. */
public class RecoverableMultipartUploadImpl implements RecoverableMultipartUpload {
    private final COSAccessHelper cosAccessHelper;

    private final Executor uploadThreadPool;

    private final Deque<CompletableFuture<PartETag>> uploadsInProgress;

    private final String namePrefixForTempObjects;

    private final MultipartUploadInfo currentUploadInfo;

    private RecoverableMultipartUploadImpl(
            COSAccessHelper cosAccessHelper,
            Executor uploadThreadPool,
            String uploadId,
            String objectName,
            List<PartETag> partsSoFar,
            long numBytes,
            Optional<File> incompletePart) {
        checkArgument(numBytes >= 0L);
        this.cosAccessHelper = cosAccessHelper;
        this.uploadThreadPool = checkNotNull(uploadThreadPool);
        this.currentUploadInfo =
                new MultipartUploadInfo(objectName, uploadId, partsSoFar, numBytes, incompletePart);
        this.namePrefixForTempObjects = createIncompletePartObjectNamePrefix(objectName);
        this.uploadsInProgress = new ArrayDeque<>();
    }

    @Override
    public void uploadPart(RefCountedFSOutputStream file) throws IOException {
        // this is to guarantee that nobody is
        // writing to the file we are uploading.
        checkState(file.isClosed());

        final CompletableFuture<PartETag> future = new CompletableFuture<>();
        uploadsInProgress.add(future);

        final long partLength = file.getPos();
        currentUploadInfo.registerNewPart(partLength);

        file.retain(); // keep the file while the async upload still runs
        uploadThreadPool.execute(
                new UploadTask(this.cosAccessHelper, currentUploadInfo, file, future));
    }

    @Override
    public RecoverableFsDataOutputStream.Committer snapshotAndGetCommitter() throws IOException {
        final COSRecoverable cosRecoverable = snapshotAndGetRecoverable(null);

        return new COSCommitter(
                this.cosAccessHelper,
                cosRecoverable.getUploadId(),
                cosRecoverable.getObjectName(),
                cosRecoverable.getPartETags(),
                cosRecoverable.getNumBytesInParts());
    }

    @Override
    public COSRecoverable snapshotAndGetRecoverable(
            @Nullable RefCountedFSOutputStream incompletePartFile) throws IOException {
        final String incompletePartObjectName = safelyUploadSmallPart(incompletePartFile);

        awaitPendingPartsUpload();

        final String objectName = this.currentUploadInfo.getObjectName();
        final String uploadId = this.currentUploadInfo.getUploadId();
        final List<PartETag> completedParts =
                this.currentUploadInfo.getCopyOfEtagsOfCompleteParts();
        final long sizeInBytes = this.currentUploadInfo.getExpectedSizeInBytes();

        if (null == incompletePartObjectName) {
            return new COSRecoverable(uploadId, objectName, completedParts, sizeInBytes);
        } else {
            return new COSRecoverable(
                    uploadId,
                    objectName,
                    completedParts,
                    sizeInBytes,
                    incompletePartObjectName,
                    incompletePartFile.getPos());
        }
    }

    @Nullable
    private String safelyUploadSmallPart(@Nullable RefCountedFSOutputStream fsOutputStream)
            throws IOException {
        if (null == fsOutputStream || fsOutputStream.getPos() == 0L) {
            return null;
        }

        final String incompletePartObjectName = createIncompletePartObjectName();
        fsOutputStream.retain();

        try {
            this.cosAccessHelper.putObject(
                    incompletePartObjectName, fsOutputStream.getInputFile(), null);
        } finally {
            fsOutputStream.release();
        }

        return incompletePartObjectName;
    }

    static String createIncompletePartObjectNamePrefix(String objectName) {
        checkNotNull(objectName);

        final int lastSlash = objectName.lastIndexOf('/');
        final String parent;
        final String child;

        if (lastSlash == -1) {
            parent = "";
            child = objectName;
        } else {
            parent = objectName.substring(0, lastSlash + 1);
            child = objectName.substring(lastSlash + 1);
        }
        return parent + (child.isEmpty() ? "" : '_') + child + "_tmp_";
    }

    private String createIncompletePartObjectName() {
        return namePrefixForTempObjects + UUID.randomUUID().toString();
    }

    private void awaitPendingPartsUpload() throws IOException {
        checkState(this.currentUploadInfo.getRemainingParts() == this.uploadsInProgress.size());

        while (this.currentUploadInfo.getRemainingParts() > 0) {
            CompletableFuture<PartETag> next = this.uploadsInProgress.peekFirst();
            PartETag nextPart = awaitPendingPartUploadToComplete(next);
            this.currentUploadInfo.registerCompletePart(nextPart);
            this.uploadsInProgress.removeFirst();
        }
    }

    private PartETag awaitPendingPartUploadToComplete(CompletableFuture<PartETag> upload)
            throws IOException {
        final PartETag completedUploadETag;

        try {
            completedUploadETag = upload.get();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for part uploads to complete");
        } catch (ExecutionException e) {
            throw new IOException("Uploading parts failed", e.getCause());
        }
        return completedUploadETag;
    }

    public static RecoverableMultipartUploadImpl newUpload(
            final COSAccessHelper cosAccessHelper,
            final Executor uploadThreadPool,
            final String objectName)
            throws IOException {

        final String multiPartUploadId = cosAccessHelper.startMultipartUpload(objectName);

        return new RecoverableMultipartUploadImpl(
                cosAccessHelper,
                uploadThreadPool,
                multiPartUploadId,
                objectName,
                new ArrayList<>(),
                0L,
                Optional.empty());
    }

    public static RecoverableMultipartUploadImpl recoverUpload(
            final COSAccessHelper cosAccessHelper,
            final Executor uploadThreadPool,
            final String multipartUploadId,
            final String objectName,
            final List<PartETag> partsSoFar,
            final long numBytesSoFar,
            final Optional<File> incompletePart) {

        return new RecoverableMultipartUploadImpl(
                cosAccessHelper,
                uploadThreadPool,
                multipartUploadId,
                objectName,
                new ArrayList<>(partsSoFar),
                numBytesSoFar,
                incompletePart);
    }

    @Override
    public Optional<File> getIncompletePart() {
        return this.currentUploadInfo.getIncompletePart();
    }

    private static class UploadTask implements Runnable {

        private final COSAccessHelper cosAccessHelper;

        private final String objectName;

        private final String uploadId;

        private final int partNumber;

        private final RefCountedFSOutputStream file;

        private final CompletableFuture<PartETag> future;

        UploadTask(
                final COSAccessHelper cosAccessHelper,
                final MultipartUploadInfo currentUpload,
                final RefCountedFSOutputStream file,
                final CompletableFuture<PartETag> future) {

            checkNotNull(currentUpload);

            this.objectName = currentUpload.getObjectName();
            this.uploadId = currentUpload.getUploadId();
            this.partNumber = currentUpload.getNumberOfRegisteredParts();

            checkArgument(partNumber >= 1 && partNumber <= 10000);

            this.cosAccessHelper = checkNotNull(cosAccessHelper);
            this.file = checkNotNull(file);
            this.future = checkNotNull(future);
        }

        @Override
        public void run() {
            try {
                File file1;
                FileInputStream fileInputStream = new FileInputStream(file.getInputFile());
                byte[] md5Hash = DigestUtils.md5(fileInputStream);
                fileInputStream.close();
                final PartETag result =
                        this.cosAccessHelper.uploadPart(
                                objectName, uploadId, partNumber, file.getInputFile(), md5Hash);
                future.complete(result);
                file.release();
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }
    }
}
