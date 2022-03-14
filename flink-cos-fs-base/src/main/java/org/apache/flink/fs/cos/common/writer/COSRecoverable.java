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

import com.qcloud.cos.model.PartETag;
import org.apache.flink.core.fs.RecoverableWriter;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public final class COSRecoverable implements RecoverableWriter.ResumeRecoverable {

    private final String uploadId;
    private final String objectName;
    private final List<PartETag> partETags;

    private final String lastPartObject;

    private long numBytesInParts;
    private long lastPartObjectLength;

    public COSRecoverable(
            String uploadId, String objectName, List<PartETag> partETags, long numBytesInParts) {
        this(uploadId, objectName, partETags, numBytesInParts, null, -1L);
    }

    COSRecoverable(
            String uploadId,
            String objectName,
            List<PartETag> partETags,
            long numBytesInParts,
            @Nullable String lastPartObject,
            long lastPartObjectLength) {
        checkArgument(numBytesInParts >= 0L);
        checkArgument(lastPartObject == null || lastPartObjectLength > 0L);

        this.uploadId = checkNotNull(uploadId);
        this.objectName = checkNotNull(objectName);
        this.partETags = checkNotNull(partETags);
        this.numBytesInParts = numBytesInParts;

        this.lastPartObject = lastPartObject;
        this.lastPartObjectLength = lastPartObjectLength;
    }

    public String getUploadId() {
        return uploadId;
    }

    public String getObjectName() {
        return objectName;
    }

    public List<PartETag> getPartETags() {
        return partETags;
    }

    public long getNumBytesInParts() {
        return numBytesInParts;
    }

    @Nullable
    public String getInCompleteObjectName() {
        return this.lastPartObject;
    }

    public long getInCompleteObjectLength() {
        return this.lastPartObjectLength;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(128);
        buf.append("COSRecoverable: ");
        buf.append("key=").append(objectName);
        buf.append(", uploadId=").append(uploadId);
        buf.append(", bytesInParts=").append(numBytesInParts);
        buf.append(", parts=[");
        int num = 0;
        for (PartETag part : this.partETags) {
            if (0 != num++) {
                buf.append(", ");
            }
            buf.append(part.getPartNumber()).append('=').append(part.getETag());
        }
        buf.append("], trailingPart=").append(lastPartObject);
        buf.append("trailingPartLen=").append(lastPartObjectLength);

        return buf.toString();
    }
}
