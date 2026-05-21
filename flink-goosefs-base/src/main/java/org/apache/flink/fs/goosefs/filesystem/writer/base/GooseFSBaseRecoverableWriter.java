package org.apache.flink.fs.goosefs.filesystem.writer.base;

import com.qcloud.cos.goosefs.hadoop.CosNFileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.goosefs.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GooseFSBaseRecoverableWriter implements RecoverableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(GooseFSBaseRecoverableWriter.class);
  private final CosNFileSystem fileSystem;
  private final boolean supportsResume;

  public GooseFSBaseRecoverableWriter(CosNFileSystem fileSystem, boolean supportsResume) {
    LOG.debug("create the goosefs base recoverable writer");
    this.fileSystem = fileSystem;
    this.supportsResume = supportsResume;
  }

  @Override
  public RecoverableFsDataOutputStream open(Path path) throws IOException {
    LOG.debug("goosefs base recoverable writer to open");
    final org.apache.hadoop.fs.Path targetFile = HadoopUtil.toHadoopPath(path);
    final org.apache.hadoop.fs.Path tempFile = HadoopUtil.generateStagingTempFilePath(fileSystem, targetFile);
    return new GooseFSBaseRecoverableFsDataOutputStream(fileSystem, targetFile, tempFile);
  }

  @Override
  public RecoverableFsDataOutputStream recover(ResumeRecoverable resumeRecoverable) throws IOException {
    if (resumeRecoverable instanceof GooseFSBaseRecoverable) {
      LOG.debug("goosefs base recoverable writer to recover");
      return new GooseFSBaseRecoverableFsDataOutputStream(fileSystem, (GooseFSBaseRecoverable) resumeRecoverable);
    } else {
      throw new IllegalArgumentException(
          "goosefs base File System cannot recover a recoverable for another file system: " + resumeRecoverable);
    }
  }

  @Override
  public boolean requiresCleanupOfRecoverableState() {
    return false;
  }

  @Override
  public boolean cleanupRecoverableState(ResumeRecoverable resumeRecoverable) throws IOException {
    return false;
  }

  @Override
  public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable commitRecoverable) throws IOException {
    if (commitRecoverable instanceof GooseFSBaseRecoverable) {
      LOG.debug("goosefs base recover for commit");
      return new GooseFSBaseRecoverableFsDataOutputStream.GooseFSPosixCommitter(fileSystem, (GooseFSBaseRecoverable) commitRecoverable);
    }
    else {
      throw new IllegalArgumentException(
          "goosefs base File System  cannot recover a recoverable for another file system: " + commitRecoverable);
    }
  }

  @Override
  public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
    SimpleVersionedSerializer<CommitRecoverable> typedSerializer = (SimpleVersionedSerializer<CommitRecoverable>) (SimpleVersionedSerializer<?>) GooseFSBaseRecoverableSerializer.INSTANCE;
    LOG.debug("goosefs base get commit recoverable serializer");

    return typedSerializer;
  }

  @Override
  public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
    SimpleVersionedSerializer<ResumeRecoverable> typedSerializer = (SimpleVersionedSerializer<ResumeRecoverable>)
        (SimpleVersionedSerializer<?>) GooseFSBaseRecoverableSerializer.INSTANCE;
    LOG.debug("goosefs base get resume recoverable serializer");

    return typedSerializer;
  }

  @Override
  public boolean supportsResume() {
    //TODO 逻辑上讲，不支持 resume 意味着不支持Exactly-Once
    LOG.debug("goosefs base supports resume: {}", supportsResume);
    return supportsResume;
  }

}
