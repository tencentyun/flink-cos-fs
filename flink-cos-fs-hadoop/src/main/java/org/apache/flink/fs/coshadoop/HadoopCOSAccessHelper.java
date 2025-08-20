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

package org.apache.flink.fs.coshadoop;

import org.apache.flink.fs.cos.common.writer.COSAccessHelper;

import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.fs.FileMetadata;
import org.apache.hadoop.fs.NativeFileSystemStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/** The AccessHelper for the CosN NativeFileSystemStore interface. */
public class HadoopCOSAccessHelper implements COSAccessHelper {

    private final NativeFileSystemStore store;

    public HadoopCOSAccessHelper(NativeFileSystemStore store) {
        this.store = store;
    }

    @Override
    public String startMultipartUpload(String key) throws IOException {
        return this.store.getUploadId(key);
    }

    @Override
    public PartETag uploadPart(
            String key, String uploadId, int partNumber, File inputFile, byte[] md5Hash)
            throws IOException {
        return this.store.uploadPart(inputFile, key, uploadId, partNumber, md5Hash);
    }

    @Override
    public void putObject(String key, File inputFile, byte[] md5Hash) throws IOException {
        this.store.storeFile(key, inputFile, md5Hash);
    }

    @Override
    public CompleteMultipartUploadResult commitMultipartUpload(
            String key, String uploadId, List<PartETag> partETags) throws IOException {
        return this.store.completeMultipartUpload(key, uploadId, partETags);
    }

    @Override
    public boolean deleteObject(String key) throws IOException {
        this.store.delete(key);
        return true;
    }

    @Override
    public long getObject(String key, File targetLocation) throws IOException {
        long numBytes = 0L;
        try (final OutputStream outStream = new FileOutputStream(targetLocation);
                final InputStream inStream = this.store.retrieve(key); ) {
            final byte[] buffer = new byte[32 * 1024];

            int numRead;
            while ((numRead = inStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, numRead);
                numBytes += numRead;
            }
        }

        // some sanity checks
        if (numBytes != targetLocation.length()) {
            throw new IOException(
                    String.format(
                            "Error recovering writer: "
                                    + "Downloading the last data chunk file gives incorrect length. "
                                    + "File=%d bytes, Stream=%d bytes",
                            targetLocation.length(), numBytes));
        }

        return numBytes;
    }

    @Override
    public FileMetadata getObjectMetadata(String key) throws IOException {
        FileMetadata fileMetadata = this.store.retrieveMetadata(key);
        if (null != fileMetadata) {
            return fileMetadata;
        } else {
            throw new FileNotFoundException("No such file for the key '" + key + "'");
        }
    }

    @Override
    public boolean isPosixBucket() {
        return this.store.isPosixBucket();
    }

    @Override
    public void close() {
        this.store.close();
    }
}
