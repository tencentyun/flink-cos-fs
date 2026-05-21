package org.apache.flink.fs.goosefs.filesystem.writer.ufs;

import com.qcloud.cos.goosefs.hadoop.FlinkFileSinkGooseFSFileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.goosefs.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.CosNFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class GooseFSUnderFSRecoverableWriter implements RecoverableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(GooseFSUnderFSRecoverableWriter.class);
  private FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem;
  private final com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem;
  private final Configuration hadoopConfiguration;

  private CosFileSystem underFileSystem;

  public GooseFSUnderFSRecoverableWriter(FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem, com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem, Configuration hadoopConfiguration) {
    LOG.debug("create the goosefs posix recoverable writer");
    this.flinkFileSinkGooseFSFileSystem = flinkFileSinkGooseFSFileSystem;
    this.hadoopConfiguration = hadoopConfiguration;
    this.gooseFSFileSystem = gooseFSFileSystem;
  }

  @Override
  public RecoverableFsDataOutputStream open(Path path) throws IOException {
    LOG.debug("goosefs posix recoverable writer to open");

    final org.apache.hadoop.fs.Path targetFile = HadoopUtil.toHadoopPath(path);
    initializeUnderFileSystem(targetFile);
    final org.apache.hadoop.fs.Path tempFile = HadoopUtil.generateStagingTempFilePath(underFileSystem, targetFile);

    return new GooseFSUnderFSRecoverableFsDataOutputStream(flinkFileSinkGooseFSFileSystem, gooseFSFileSystem, underFileSystem, targetFile, tempFile);
  }

  @Override
  public RecoverableFsDataOutputStream recover(ResumeRecoverable resumeRecoverable) throws IOException {
    if (resumeRecoverable instanceof GooseFSUnderFSRecoverable) {
      LOG.debug("goosefs underFS recoverable writer to recover");
      initializeUnderFileSystem(((GooseFSUnderFSRecoverable) resumeRecoverable).targetFile());
      return new GooseFSUnderFSRecoverableFsDataOutputStream(flinkFileSinkGooseFSFileSystem, gooseFSFileSystem, underFileSystem, (GooseFSUnderFSRecoverable) resumeRecoverable);
    } else {
      throw new IllegalArgumentException(
          "goosefs underFS File System cannot recover a recoverable for another file system: " + resumeRecoverable);
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
    if (commitRecoverable instanceof GooseFSUnderFSRecoverable) {
      LOG.debug("goosefs underFS recover for commit");
      initializeUnderFileSystem(((GooseFSUnderFSRecoverable) commitRecoverable).targetFile());
      return new GooseFSUnderFSRecoverableFsDataOutputStream.GooseFSUnderFSCommitter(flinkFileSinkGooseFSFileSystem, gooseFSFileSystem, underFileSystem, (GooseFSUnderFSRecoverable) commitRecoverable);
    } else {
      throw new IllegalArgumentException(
          "goosefs underFS File System  cannot recover a recoverable for another file system: " + commitRecoverable);
    }
  }

  @Override
  public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
    SimpleVersionedSerializer<CommitRecoverable> typedSerializer = (SimpleVersionedSerializer<CommitRecoverable>) (SimpleVersionedSerializer<?>) GooseFSUnderFSRecoverableSerializer.INSTANCE;
    LOG.debug("goosefs underFS get commit recoverable serializer");

    return typedSerializer;
  }

  @Override
  public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
    SimpleVersionedSerializer<ResumeRecoverable> typedSerializer = (SimpleVersionedSerializer<ResumeRecoverable>)
        (SimpleVersionedSerializer<?>) GooseFSUnderFSRecoverableSerializer.INSTANCE;
    LOG.debug("goosefs underFS get resume recoverable serializer");

    return typedSerializer;
  }

  @Override
  public boolean supportsResume() {
    LOG.debug("goosefs underFS supports resume");
    return true;
  }

  private synchronized void initializeUnderFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
    if (null != this.underFileSystem) {
      LOG.info("The under filesystem has been initialize.");
      return;
    }

    if (this.hadoopConfiguration == null) {
      LOG.error("The initializeURI and the configuration must not be null. " +
          "initializeURI: {}, configuration: {}.", path, hadoopConfiguration);
      throw new IOException("Initialize the under filesystem failed.");
    }

    LOG.info("Initializing the under filesystem...");
    this.underFileSystem = this.retrieveUnderFileSystem(path.toUri(), hadoopConfiguration);
  }

  protected CosFileSystem retrieveUnderFileSystem(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String rawAuthority = uri.getRawAuthority();
    String rawPath = uri.getRawPath();
    URI newUri = URI.create(scheme + "://" + rawAuthority + "/" + rawPath);
    LOG.debug("cos uri: {}", newUri);
    CosFileSystem fileSystem = new CosFileSystem();
    fileSystem.initialize(newUri, conf);
    return fileSystem;
  }

}
