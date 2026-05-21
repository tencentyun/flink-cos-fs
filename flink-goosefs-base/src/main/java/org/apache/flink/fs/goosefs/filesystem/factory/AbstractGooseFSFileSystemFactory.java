package org.apache.flink.fs.goosefs.filesystem.factory;

import com.qcloud.cos.goosefs.hadoop.CosNFileSystem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;
import org.apache.flink.fs.goosefs.constant.Constants;
import org.apache.flink.fs.goosefs.constant.FileSystemType;
import org.apache.flink.fs.goosefs.filesystem.writer.base.GooseFSBaseFileSystem;
import org.apache.flink.fs.goosefs.filesystem.writer.ufs.GooseFSUnderFSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractGooseFSFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractGooseFSFileSystemFactory.class);

  private Configuration flinkConfiguration;
  private org.apache.hadoop.conf.Configuration hadoopConfiguration;


  public FileSystem create(URI fsUri) throws IOException {
    checkNotNull(fsUri, "fsUri");

    final String scheme = fsUri.getScheme();
    checkArgument(scheme != null, "file system has null scheme");
    checkArgument(scheme.equals(getScheme()), String.format("file system scheme must not match the factory scheme: %s", scheme));

    try {
      final CosNFileSystem goosefs = new CosNFileSystem();

      final URI initUri;
      if (fsUri.getAuthority() != null) {
        initUri = fsUri;
      } else {
        throw new IOException(String.format("Authority not exist, uri : %s", fsUri));
      }

      try {
        goosefs.initialize(initUri, hadoopConfiguration);
      } catch (UnknownHostException e) {
        String message =
            "The Hadoop file system's authority ("
                + initUri.getAuthority()
                + "), specified by either the file URI or the configuration, cannot be resolved.";

        throw new IOException(message, e);
      }

      String type = flinkConfiguration.getString(Constants.FILESYSTEM_TYPE_KEY, FileSystemType.UFS.name());
      if (Objects.equals(type, FileSystemType.GFS.name())) {
        LOG.info("Use goosefs base file system");
        return new GooseFSBaseFileSystem(goosefs, hadoopConfiguration);
      } else if (Objects.equals(type, FileSystemType.UFS.name())) {
        LOG.info("Use goosefs under file system");
        return new GooseFSUnderFSFileSystem(goosefs, hadoopConfiguration);
      } else {
        throw new IOException("Unsupported file system type: " + type);
      }
    } catch (LinkageError e) {
      throw new UnsupportedFileSystemSchemeException(
          "Cannot support file system for '"
              + fsUri.getScheme()
              + "' via Hadoop, because Hadoop is not in the classpath, or some classes "
              + "are missing from the classpath.",
          e);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Cannot instantiate file system for URI: " + fsUri, e);
    }
  }

  protected abstract String getScheme();

//  private boolean isPosixBucket(URI fsUri) throws IOException {
//    String scheme = fsUri.getScheme();
//    String bucket = fsUri.getHost();
//
//    checkArgument(!scheme.equals(getScheme()), "file system scheme must not match the factory scheme");
//
//    if (Objects.equals(getScheme(), "ofs")) {
//      return true;
//    } else if (Objects.equals(getScheme(), "cosn")) {
//      CosFileSystem cosFileSystem = new CosFileSystem();
//      boolean isPosixBucket = cosFileSystem.getStore().headBucket(bucket).isMergeBucket();
//      boolean isPosixProcess = false;
//
//      String bucketImpl = hadoopConfiguration.get(CosNConfigKeys.COSN_POSIX_BUCKET_FS_IMPL);
//      if (isPosixBucket) {
//        if (null != bucketImpl) {
//          if (bucketImpl.equals(CosNConfigKeys.DEFAULT_COSN_POSIX_BUCKET_FS_IMPL)) {
//            isPosixProcess = true;
//          }
//        } else {
//          isPosixProcess = true;
//        }
//      }
//      LOG.info("Creating the Flink cos file system, " +
//              "create posix process recover writer: {}, " +
//              "bucket {} is posix bucket: {}, bucket impl {}.",
//          isPosixProcess, bucket, isPosixBucket, bucketImpl);
//
//      return isPosixProcess;
//    } else {
//      return false;
//    }
//  }


  public void configure(Configuration config, org.apache.hadoop.conf.Configuration hadoopConfiguration) {
    this.flinkConfiguration = config;
    this.hadoopConfiguration = hadoopConfiguration;
  }
}
