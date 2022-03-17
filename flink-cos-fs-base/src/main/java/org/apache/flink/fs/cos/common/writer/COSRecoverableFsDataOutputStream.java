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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.cos.common.FlinkCOSFileSystem;
import org.apache.flink.fs.cos.common.utils.RefCountedBufferingFileStream;
import org.apache.flink.fs.cos.common.utils.RefCountedFSOutputStream;
import org.apache.flink.fs.cos.common.utils.RefCountedFile;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.commons.io.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkArgument;

/** COSRecoverableFsDataOutputStream. */
@PublicEvolving
@NotThreadSafe
public class COSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {
    private final ReentrantLock lock = new ReentrantLock();

    private final RecoverableMultipartUpload upload;

    private final FunctionWithException<File, RefCountedFile, IOException> tmpFileProvider;

    private final long userDefinedMinPartSize;

    private RefCountedFSOutputStream fileStream;

    private long bytesBeforeCurrentPart;

    public COSRecoverableFsDataOutputStream(
            RecoverableMultipartUpload upload,
            FunctionWithException<File, RefCountedFile, IOException> tmpFileProvider,
            RefCountedFSOutputStream initialTmpFile,
            long userDefinedMinPartSize,
            long bytesBeforeCurrentPart) {

        checkArgument(bytesBeforeCurrentPart >= 0L);

        this.upload = upload;
        this.tmpFileProvider = tmpFileProvider;
        this.userDefinedMinPartSize = userDefinedMinPartSize;
        this.fileStream = initialTmpFile;
        this.bytesBeforeCurrentPart = bytesBeforeCurrentPart;
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        this.lock();
        try {
            this.fileStream.flush();
            openNewPartIfNecessary(userDefinedMinPartSize);

            // We do not stop writing to the current file, we merely limit the upload to the
            // first n bytes of the current file

            return upload.snapshotAndGetRecoverable(fileStream);
        } finally {
            unlock();
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        lock();
        try {
            closeAndUploadPart();
            return upload.snapshotAndGetCommitter();
        } finally {
            unlock();
        }
    }

    @Override
    public long getPos() throws IOException {
        return bytesBeforeCurrentPart + fileStream.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        fileStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fileStream.write(b, off, len);
        openNewPartIfNecessary(userDefinedMinPartSize);
    }

    @Override
    public void flush() throws IOException {
        fileStream.flush();
        openNewPartIfNecessary(userDefinedMinPartSize);
    }

    @Override
    public void sync() throws IOException {
        fileStream.sync();
    }

    @Override
    public void close() throws IOException {
        lock();
        try {
            fileStream.flush();
        } finally {
            IOUtils.closeQuietly(fileStream);
            fileStream.release();
            unlock();
        }
    }

    private void openNewPartIfNecessary(long sizeThreshold) throws IOException {
        final long fileLength = fileStream.getPos();
        if (fileLength >= sizeThreshold) {
            lock();
            try {
                uploadCurrentAndOpenNewPart(fileLength);
            } finally {
                unlock();
            }
        }
    }

    private void uploadCurrentAndOpenNewPart(long fileLength) throws IOException {
        bytesBeforeCurrentPart += fileLength;
        closeAndUploadPart();

        // initialize a new temp file
        fileStream = RefCountedBufferingFileStream.openNew(tmpFileProvider);
    }

    private void closeAndUploadPart() throws IOException {
        fileStream.flush();
        fileStream.close();
        if (fileStream.getPos() > 0L) {
            upload.uploadPart(fileStream);
        }
        fileStream.release();
    }

    private void lock() throws IOException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        }
    }

    private void unlock() {
        lock.unlock();
    }

    public static COSRecoverableFsDataOutputStream newStream(
            final RecoverableMultipartUpload upload,
            final FunctionWithException<File, RefCountedFile, IOException> tmpFileCreator,
            final long userDefinedMinPartSize)
            throws IOException {

        checkArgument(
                userDefinedMinPartSize >= FlinkCOSFileSystem.COS_MULTIPART_UPLOAD_PART_MIN_SIZE);

        final RefCountedBufferingFileStream fileStream =
                boundedBufferingFileStream(tmpFileCreator, Optional.empty());

        return new COSRecoverableFsDataOutputStream(
                upload, tmpFileCreator, fileStream, userDefinedMinPartSize, 0L);
    }

    public static COSRecoverableFsDataOutputStream recoverStream(
            final RecoverableMultipartUpload upload,
            final FunctionWithException<File, RefCountedFile, IOException> tmpFileCreator,
            final long userDefinedMinPartSize,
            final long bytesBeforeCurrentPart)
            throws IOException {

        checkArgument(
                userDefinedMinPartSize >= FlinkCOSFileSystem.COS_MULTIPART_UPLOAD_PART_MIN_SIZE);

        final RefCountedBufferingFileStream fileStream =
                boundedBufferingFileStream(tmpFileCreator, upload.getIncompletePart());

        return new COSRecoverableFsDataOutputStream(
                upload, tmpFileCreator, fileStream, userDefinedMinPartSize, bytesBeforeCurrentPart);
    }

    private static RefCountedBufferingFileStream boundedBufferingFileStream(
            final FunctionWithException<File, RefCountedFile, IOException> tmpFileCreator,
            final Optional<File> incompletePart)
            throws IOException {

        if (!incompletePart.isPresent()) {
            return RefCountedBufferingFileStream.openNew(tmpFileCreator);
        }

        final File file = incompletePart.get();
        return RefCountedBufferingFileStream.restore(tmpFileCreator, file);
    }
}
