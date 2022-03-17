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
import org.apache.flink.core.fs.RecoverableWriter;

import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.fs.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** The COSCommitter for implementing the exactly-once. */
public class COSCommitter implements RecoverableFsDataOutputStream.Committer {
    private static final Logger LOG = LoggerFactory.getLogger(COSCommitter.class);

    private final COSAccessHelper cosAccessHelper;
    private final String uploadId;
    private final String objectName;
    private final List<PartETag> partETags;

    private final long totalLength;

    public COSCommitter(
            COSAccessHelper cosAccessHelper,
            String uploadId,
            String objectName,
            List<PartETag> partETags,
            long totalLength) {
        this.cosAccessHelper = cosAccessHelper;
        this.uploadId = uploadId;
        this.objectName = objectName;
        this.partETags = partETags;
        this.totalLength = totalLength;
    }

    @Override
    public void commit() throws IOException {
        if (totalLength > 0L) {
            LOG.info("Committing {} with MultipartUpload ID: {}.", this.objectName, this.uploadId);

            final AtomicInteger errorCount = new AtomicInteger();
            this.cosAccessHelper.commitMultipartUpload(
                    this.objectName, this.uploadId, this.partETags);
        } else {
            LOG.debug("No data to commit for the file: {}.", this.objectName);
        }
    }

    @Override
    public void commitAfterRecovery() throws IOException {
        if (this.totalLength > 0L) {
            LOG.info(
                    "Trying to commit after recovery {} with the MultipartUpload ID: {}.",
                    this.objectName,
                    this.uploadId);

            try {
                this.cosAccessHelper.commitMultipartUpload(
                        this.objectName, this.uploadId, this.partETags);
            } catch (IOException e) {
                LOG.info(
                        "Failed to commit after recovery {} with "
                                + "the MultipartUpload ID: {}. Checking if the file was committed before...",
                        this.objectName,
                        this.uploadId);
                LOG.trace("Exception when committing.", e);

                try {
                    FileMetadata fileMetadata =
                            this.cosAccessHelper.getObjectMetadata(this.objectName);

                    if (this.totalLength != fileMetadata.getLength()) {
                        String message =
                                String.format(
                                        "Inconsistent result for object %s: conflicting lengths. "
                                                + "Recovered committer for upload %s indicates %s bytes, present object is %s bytes",
                                        objectName,
                                        uploadId,
                                        totalLength,
                                        fileMetadata.getLength());
                        LOG.warn(message);
                        throw new IOException(message, e);
                    }
                } catch (FileNotFoundException fileNotFoundException) {
                    LOG.warn(
                            "Object {} not existing after failed recovery commit with MPU ID {}",
                            this.objectName,
                            this.uploadId);
                    throw new IOException(
                            String.format(
                                    "Recovering commit failed for object %s. "
                                            + "Object does not exist and MultiPart Upload %s is not valid.",
                                    this.objectName, this.uploadId),
                            e);
                }
            }
        } else {
            LOG.debug("No data to commit for file: {}.", this.objectName);
        }
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
        return new COSRecoverable(this.uploadId, this.objectName, this.partETags, this.totalLength);
    }
}
