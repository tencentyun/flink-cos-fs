package org.apache.flink.fs.goosefs.filesystem.writer.base;

import com.qcloud.cos.goosefs.hadoop.CosNFileSystem;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.goosefs.util.HadoopUtil;
import org.apache.flink.util.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class GooseFSBaseRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GooseFSBaseRecoverableFsDataOutputStream.class);

  private final CosNFileSystem fs;

  private final Path targetFile;

  private final Path originTempFile;
  private final Path currentTempFile;

  private final FSDataOutputStream out;

  public GooseFSBaseRecoverableFsDataOutputStream(CosNFileSystem fs, Path targetFile, Path tempFile) throws IOException {
    LOG.debug("goosefs base recoverable fs data output stream " +
        "temp file recover way trigger, file: {}", tempFile.toString());
    this.fs = fs;
    this.targetFile = targetFile;
    this.originTempFile = tempFile;
    this.currentTempFile = tempFile;
    this.out = fs.create(currentTempFile);
  }

  public GooseFSBaseRecoverableFsDataOutputStream(CosNFileSystem fs, GooseFSBaseRecoverable recoverable) throws IOException {
    this.fs = fs;
    this.targetFile = checkNotNull(recoverable.targetFile());
    this.originTempFile = checkNotNull(recoverable.tempFile());
    this.currentTempFile = HadoopUtil.generateStagingTempFilePath(fs, targetFile);

    LOG.debug("goosefs base recoverable fs data output stream " +
        "temp file recover way trigger, originTempFile: {}, currentTempFile: {}", originTempFile.toString(), currentTempFile.toString());

    try (FSDataInputStream in = fs.open(originTempFile)) {
      this.out = fs.create(currentTempFile);
      safelyCopyFile(in, out, recoverable.offset());
    }

    long pos = out.getPos();
    if (pos != recoverable.offset()) {
      IOUtils.closeQuietly(out);
      throw new IOException("Mock Truncate failed: " + originTempFile + " --> " + currentTempFile +
          " (requested=" + recoverable.offset() + " ,size=" + pos + ")");
    }
  }

  @Override
  public RecoverableWriter.ResumeRecoverable persist() throws IOException {
    close();
    LOG.debug("goosefs base persist target:{}, temp:{}, pos:{}", targetFile.toString(),
        currentTempFile.toString(), getPos());
    return new GooseFSBaseRecoverable(targetFile, currentTempFile, getPos());
  }

  @Override
  public Committer closeForCommit() throws IOException {
    final long pos = getPos();
    close();
    LOG.debug("goosefs base close for commit target:{}, temp:{}, pos:{}", targetFile.toString(),
        currentTempFile.toString(), pos);
    return new GooseFSPosixCommitter(fs,
        new GooseFSBaseRecoverable(targetFile, currentTempFile, pos));
  }

  @Override
  public long getPos() throws IOException {
    return out.getPos();
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void sync() throws IOException {
    out.flush();
    out.hsync();
  }

  @Override
  public void close() throws IOException {
    LOG.debug("goosefs base calling close file:{}, pos:{}", currentTempFile.toString(), out.getPos());
    out.close();
  }

  private static void safelyCopyFile(
      final FSDataInputStream in,
      final FSDataOutputStream out,
      final Long offset) throws IOException {

    // 参数校验
    if (in == null || out == null || offset == null) {
      throw new IllegalArgumentException("Input stream, output stream and offset must not be null");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be >= 0, but was: " + offset);
    }

    final byte[] buffer = new byte[1024 * 1024]; // 1MB 缓冲区，可调整
    long bytesRemaining = offset;
    int bytesRead;

    while (bytesRemaining > 0) {
      int bytesToRead = (int) Math.min(buffer.length, bytesRemaining);

      bytesRead = in.read(buffer, 0, bytesToRead);

      if (bytesRead == -1) {
        throw new IOException(String.format(
            "Unexpected end of input stream: tried to read %d bytes, but only %d were available.",
            offset, (offset - bytesRemaining + bytesRead)
        ));
      }

      out.write(buffer, 0, bytesRead);
      bytesRemaining -= bytesRead;
    }

    out.flush();
  }

  static class GooseFSPosixCommitter implements Committer {
    private static final Logger LOG = LoggerFactory.getLogger(GooseFSPosixCommitter.class);

    private final CosNFileSystem fs;
    private final GooseFSBaseRecoverable recoverable;

    GooseFSPosixCommitter(CosNFileSystem fs, GooseFSBaseRecoverable recoverable) {
      this.fs = checkNotNull(fs);
      this.recoverable = checkNotNull(recoverable);
    }

    @Override
    public void commit() throws IOException {
      LOG.debug("goosefs base committer begin commit");
      final Path src = recoverable.tempFile();
      final Path dest = recoverable.targetFile();
      final long expectedLength = recoverable.offset();

      final FileStatus srcStatus;
      try {
        srcStatus = fs.getFileStatus(src);
      } catch (IOException e) {
        LOG.info("goosefs base output stream commit failed, src not exist {}, {}", src.toString(), dest.toString());
        throw new IOException("Cannot clean commit: Staging file does not exist.");
      }

      if (srcStatus.getLen() != expectedLength) {
        // something was done to this file since the committer was created.
        // this is not the "clean" case
        throw new IOException("Cannot clean commit: File has trailing junk data.");
      }

      try {
        LOG.info("goosefs base output stream commit {}, {}", src.toString(), dest.toString());
        fs.rename(src, dest);
      } catch (IOException e) {
        throw new IOException("Committing file by rename failed: " + src + " to " + dest, e);
      }
    }

    @Override
    public void commitAfterRecovery() throws IOException {
      LOG.debug("goosefs base committer begin commit after recovery");
      final Path src = recoverable.tempFile();
      final Path dest = recoverable.targetFile();
      final long expectedLength = recoverable.offset();

      FileStatus srcStatus = null;
      try {
        srcStatus = fs.getFileStatus(src);
      } catch (FileNotFoundException e) {
        // status remains null
      } catch (IOException e) {
        throw new IOException("Committing during recovery failed: Could not access status of source file.");
      }

      if (srcStatus != null) {
        Path currentTempFile = null;
        if (srcStatus.getLen() > expectedLength) {
          // can happen if we go from persist to recovering for commit directly
          // truncate the trailing junk away
          LOG.info("goosefs base commit after recovery process, src:{}, recover:{}",
              src.toString(), recoverable);

          currentTempFile = HadoopUtil.generateStagingTempFilePath(fs, dest);

          try (FSDataInputStream in = fs.open(src);
               FSDataOutputStream out = fs.create(currentTempFile)) {
            safelyCopyFile(in, out, recoverable.offset());
          }
        }

        // rename to final location (if it exists, overwrite it)
        try {
          LOG.info("goosefs base output stream commit after recovery {}, {}", src.toString(), dest.toString());
          if (currentTempFile!= null) {
            fs.rename(currentTempFile, dest);
          } else {
            fs.rename(src, dest);
          }
        } catch (IOException e) {
          throw new IOException("Committing file by rename failed: " + src + " to " + dest, e);
        }
      } else if (!fs.exists(dest)) {
        LOG.warn("rename {} to {}.\n" +
            "neither exists - that can be a sign of\n" +
            "           - (1) a serious problem (file system loss of data)\n" +
            "           - (2) a recovery of a savepoint that is some time old and the users\n" +
            "                 removed the files in the meantime.", src, dest);

        // TODO how to handle this?
        // We probably need an option for users whether this should log,
        // or result in an exception or unrecoverable exception
      }
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
      return recoverable;
    }
  }

}
