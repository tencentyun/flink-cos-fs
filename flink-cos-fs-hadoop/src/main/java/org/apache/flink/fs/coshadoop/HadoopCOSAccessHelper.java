package org.apache.flink.fs.coshadoop;

import com.qcloud.cos.model.*;
import org.apache.flink.fs.cos.common.writer.COSAccessHelper;
import org.apache.hadoop.fs.FileMetadata;
import org.apache.hadoop.fs.NativeFileSystemStore;

import java.io.*;
import java.util.List;

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
	public PartETag uploadPart(String key, String uploadId, int partNumber, File inputFile, byte[] md5Hash) throws IOException {
		return this.store.uploadPart(inputFile, key, uploadId, partNumber, md5Hash);
	}

	@Override
	public void putObject(String key, File inputFile, byte[] md5Hash) throws IOException {
		this.store.storeFile(key, inputFile, md5Hash);
	}

	@Override
	public CompleteMultipartUploadResult commitMultipartUpload(String key,
        String uploadId, List<PartETag> partETags) throws IOException {
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
		try (
			final OutputStream outStream = new FileOutputStream(targetLocation);
			final InputStream inStream = this.store.retrieve(key);
		) {
			final byte[] buffer = new byte[32 * 1024];

			int numRead;
			while ((numRead = inStream.read(buffer)) != -1) {
				outStream.write(buffer, 0, numRead);
				numBytes += numRead;
			}
		}

		// some sanity checks
		if (numBytes != targetLocation.length()) {
			throw new IOException(String.format("Error recovering writer: " +
					"Downloading the last data chunk file gives incorrect length. " +
					"File=%d bytes, Stream=%d bytes",
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
}
