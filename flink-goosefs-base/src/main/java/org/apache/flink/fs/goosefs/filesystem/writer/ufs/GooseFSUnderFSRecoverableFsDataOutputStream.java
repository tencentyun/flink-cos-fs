package org.apache.flink.fs.goosefs.filesystem.writer.ufs;

import com.qcloud.chdfs.fs.CHDFSHadoopFileSystem;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.qcloud.cos.goosefs.GooseFSURI;
import com.qcloud.cos.goosefs.conf.InstancedConfiguration;
import com.qcloud.cos.goosefs.exception.FileDoesNotExistException;
import com.qcloud.cos.goosefs.exception.GooseFSException;
import com.qcloud.cos.goosefs.grpc.FileSystemMasterCommonPOptions;
import com.qcloud.cos.goosefs.grpc.ListStatusPOptions;
import com.qcloud.cos.goosefs.grpc.LoadMetadataPType;
import com.qcloud.cos.goosefs.hadoop.FlinkFileSinkGooseFSFileSystem;
import com.qcloud.cos.goosefs.util.FileSystemOptions;

import org.apache.hadoop.fs.CosFileSystem;

import org.apache.hadoop.fs.CosNFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * GooseFS 底层文件系统可恢复数据输出流实现类。
 * 该类实现了 Flink 的可恢复数据输出流接口，用于支持 GooseFS 文件系统的可恢复写入操作。
 */
public class GooseFSUnderFSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GooseFSUnderFSRecoverableFsDataOutputStream.class);

  private static Method truncateHandle;

  private final com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem;

  private final FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem;

  private CHDFSHadoopFileSystem underFileSystem;

  private final Path targetFile;

  private final Path currentTempFile;

  private final FSDataOutputStream out;

  public GooseFSUnderFSRecoverableFsDataOutputStream(FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem, com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem, CHDFSHadoopFileSystem underFileSystem, Path targetFile, Path tempFile) throws IOException {
    LOG.debug("cos merge recoverable fs data output stream " +
        "temp file recover way trigger, file: {}", tempFile.toString());
    this.flinkFileSinkGooseFSFileSystem = checkNotNull(flinkFileSinkGooseFSFileSystem);
    this.gooseFSFileSystem = checkNotNull(gooseFSFileSystem);
    this.underFileSystem = checkNotNull(underFileSystem);

    ensureTruncateInitialized();
    this.targetFile = targetFile;
    this.currentTempFile = tempFile;
    this.out = underFileSystem.create(currentTempFile);
  }

  public GooseFSUnderFSRecoverableFsDataOutputStream(FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem, com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem, CHDFSHadoopFileSystem underFileSystem, GooseFSUnderFSRecoverable recoverable) throws IOException {
    //goosefsBaseFileSystem， 仅用作特殊情况下的 LoadMetadata
    this.flinkFileSinkGooseFSFileSystem = checkNotNull(flinkFileSinkGooseFSFileSystem);
    this.gooseFSFileSystem = checkNotNull(gooseFSFileSystem);
    //underFileSystem 用于特殊情况下的 append 和 truncate 语义
    this.underFileSystem = checkNotNull(underFileSystem);
    this.targetFile = checkNotNull(recoverable.targetFile());
    this.currentTempFile = checkNotNull(recoverable.tempFile());

    ensureTruncateInitialized();
    LOG.debug("cos merge recoverable fs data output stream " +
        "temp file recover way trigger,  currentTempFile: {}", currentTempFile.toString());

    safelyTruncateFile(this.underFileSystem, currentTempFile, recoverable);

    out = this.underFileSystem.append(currentTempFile);
    LOG.debug("after truncate and append file:{}, pos:{}", currentTempFile.toString(), out.getPos());

    long pos = out.getPos();
    if (pos != recoverable.offset()) {
      IOUtils.closeQuietly(out);
      throw new IOException("Mock Truncate failed: " + currentTempFile +
          " (requested=" + recoverable.offset() + " ,size=" + pos + ")");
    }
  }

  @Override
  public RecoverableWriter.ResumeRecoverable persist() throws IOException {
    sync();
    LOG.debug("cos merge persist target:{}, temp:{}, pos:{}", targetFile.toString(),
        currentTempFile.toString(), getPos());
    return new GooseFSUnderFSRecoverable(targetFile, currentTempFile, getPos());
  }

  @Override
  public Committer closeForCommit() throws IOException {
    final long pos = getPos();
    close();
    LOG.debug("cos merge close for commit target:{}, temp:{}, pos:{}", targetFile.toString(),
        currentTempFile.toString(), pos);
    return new GooseFSUnderFSCommitter(flinkFileSinkGooseFSFileSystem, gooseFSFileSystem, underFileSystem,
        new GooseFSUnderFSRecoverable(targetFile, currentTempFile, pos));
  }

  @Override
  public long getPos() {
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

  private static void safelyTruncateFile(
      final CHDFSHadoopFileSystem fileSystem,
      final Path path,
      final GooseFSUnderFSRecoverable recoverable) throws IOException {

    ensureTruncateInitialized();

    // if fd or session not close when occur something interrupt,
    // the truncate will try to open fd again which may occur the 'can not open fd again'
    // so every time begin call the truncate, manual to unlock the fd.
    fileSystem.releaseFileLock(path);

    try {
      LOG.debug("goosefs base safely truncate file path:{}, offset:{}", path.toString(), recoverable.offset());
      truncate(fileSystem, path, recoverable.offset());
    } catch (Exception e) {
      throw new IOException("Problem while truncating file: " + path, e);
    }

    // todo whether need the same recover lease to the hdfs file system
  }

  private static void ensureTruncateInitialized() throws FlinkRuntimeException {
    Method truncateMethod;
    try {
      truncateMethod = FileSystem.class.getMethod("truncate", Path.class, long.class);
    } catch (NoSuchMethodException e) {
      throw new FlinkRuntimeException("Could not find a public truncate method on the Hadoop File System.");
    }

    if (!Modifier.isPublic(truncateMethod.getModifiers())) {
      throw new FlinkRuntimeException("Could not find a public truncate method on the Hadoop File System.");
    }
    LOG.debug("ensure truncate initialized is ok");
    truncateHandle = truncateMethod;
  }

  private static boolean truncate(final CHDFSHadoopFileSystem hadoopFs, final Path file, final long length) throws IOException {
    if (truncateHandle != null) {
      try {
        return (Boolean) truncateHandle.invoke(hadoopFs, file, length);
      } catch (InvocationTargetException e) {
        ExceptionUtils.rethrowIOException(e.getTargetException());
      } catch (Throwable t) {
        throw new IOException(
            "Truncation of file failed because of access/linking problems with Hadoop's truncate call. " +
                "This is most likely a dependency conflict or class loading problem.");
      }
    } else {
      throw new IllegalStateException("Truncation handle has not been initialized");
    }
    return false;
  }

  static class GooseFSUnderFSCommitter implements Committer {
    private static final Logger LOG = LoggerFactory.getLogger(GooseFSUnderFSCommitter.class);

    private final com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem;

    private final FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem;
    private CHDFSHadoopFileSystem underFileSystem;
    private final GooseFSUnderFSRecoverable recoverable;

    GooseFSUnderFSCommitter(FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem, com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem, CHDFSHadoopFileSystem underFileSystem, GooseFSUnderFSRecoverable recoverable) {
      //hcfs 语义的goosefs 文件系统，用作常规的文件创建
      //goosefsBaseFileSystem， 仅用作特殊情况下的 LoadMetadata
      ensureTruncateInitialized();
      this.flinkFileSinkGooseFSFileSystem = checkNotNull(flinkFileSinkGooseFSFileSystem);
      this.gooseFSFileSystem = checkNotNull(gooseFSFileSystem);
      //underFileSystem 用于特殊情况下的 append 和 truncate 语义
      this.underFileSystem = checkNotNull(underFileSystem);
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
        srcStatus = underFileSystem.getFileStatus(src);
      } catch (IOException e) {
        LOG.info("goosefs base output stream commit failed, src not exist {}, {}", src.toString(), dest.toString());
        throw new IOException(String.format("Cannot clean commit: Staging file does not exist. %s, %s", src, dest));
      }

      if (srcStatus.getLen() != expectedLength) {
        // something was done to this file since the committer was created.
        // this is not the "clean" case
        throw new IOException(String.format("Cannot clean commit: File has trailing junk data. %s, %s", srcStatus.getLen(), expectedLength));
      }

      try {
        LOG.info("goosefs base output stream commit after recovery {}, {}", src.toString(), dest.toString());
        //如果不是降级模式，向 goosefs 同步元数据
        if (underFileSystem.exists(src) && !flinkFileSinkGooseFSFileSystem.isAccessUnderFileSystem(src)) {
          syncGooseFS(src);
          syncGooseFS(dest);
          GooseFSURI gfsSrc = flinkFileSinkGooseFSFileSystem.getGooseFSPath(src);
          GooseFSURI gfsDesc = flinkFileSinkGooseFSFileSystem.getGooseFSPath(dest);
          if (gooseFSFileSystem.exists(gfsDesc)) {
            gooseFSFileSystem.delete(gfsDesc);
          }
          gooseFSFileSystem.rename(gfsSrc, gfsDesc);
        } else {
          underFileSystem.rename(src, dest);
        }
      } catch (IOException | GooseFSException e) {
        throw new IOException("Committing file by rename failed: " + src + " to " + dest, e);
      }
    }

    private void syncGooseFS(Path dest) throws IOException {
      try {
        GooseFSURI gfsDest = flinkFileSinkGooseFSFileSystem.getGooseFSPath(dest);

        ListStatusPOptions options = FileSystemOptions.listStatusDefaults(InstancedConfiguration.defaults()).toBuilder()
            .setRecursive(false)
            .setLoadMetadataType(LoadMetadataPType.ALWAYS)
            .setIsLoadMetadata(true)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                .setSyncIntervalMs(0).build()).build();

        gooseFSFileSystem.loadMetadata(gfsDest, options);
      } catch (FileDoesNotExistException e) {
        LOG.debug("loadMetadata file not exist: {}", dest);
      } catch (GooseFSException e) {
        throw new IOException(e);
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
        srcStatus = underFileSystem.getFileStatus(src);
      } catch (FileNotFoundException e) {
        // status remains null
      } catch (IOException e) {
        throw new IOException("Committing during recovery failed: Could not access status of source file.");
      }

      if (srcStatus != null) {
        if (srcStatus.getLen() > expectedLength) {
          // can happen if we go from persist to recovering for commit directly
          // truncate the trailing junk away
          LOG.info("goosefs base commit after recovery process, src:{}, recover:{}",
              src.toString(), recoverable);
          ensureTruncateInitialized();
          safelyTruncateFile(this.underFileSystem, src, recoverable);
        }

        // rename to final location (if it exists, overwrite it)
        try {
          LOG.info("goosefs base output stream commit after recovery {}, {}", src.toString(), dest.toString());
          //如果不是降级模式，向 goosefs 同步元数据
          if (underFileSystem.exists(src) && !flinkFileSinkGooseFSFileSystem.isAccessUnderFileSystem(src)) {
            syncGooseFS(src);
            syncGooseFS(dest);
            GooseFSURI gfsSrc = flinkFileSinkGooseFSFileSystem.getGooseFSPath(src);
            GooseFSURI gfsDesc = flinkFileSinkGooseFSFileSystem.getGooseFSPath(dest);
            if (gooseFSFileSystem.exists(gfsDesc)) {
              gooseFSFileSystem.delete(gfsDesc);
            }
            gooseFSFileSystem.rename(gfsSrc, gfsDesc);
          } else {
            underFileSystem.rename(src, dest);
          }
        } catch (IOException | GooseFSException e) {
          throw new IOException("Committing file by rename failed: " + src + " to " + dest, e);
        }

      } else if (!underFileSystem.exists(dest)) {
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