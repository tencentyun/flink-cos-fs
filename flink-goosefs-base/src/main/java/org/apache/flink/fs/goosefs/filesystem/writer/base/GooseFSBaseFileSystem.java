package org.apache.flink.fs.goosefs.filesystem.writer.base;


import com.qcloud.cos.goosefs.hadoop.CosNFileSystem;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.goosefs.constant.Constants;
import org.apache.flink.runtime.fs.hdfs.HadoopBlockLocation;
import org.apache.flink.runtime.fs.hdfs.HadoopFileStatus;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;


public class GooseFSBaseFileSystem extends HadoopFileSystem {

  private final CosNFileSystem fileSystem;
  private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

  public GooseFSBaseFileSystem(CosNFileSystem fileSystem, org.apache.hadoop.conf.Configuration hadoopConfiguration) {
    super(fileSystem);
    this.fileSystem = checkNotNull(fileSystem);
    this.hadoopConfiguration = checkNotNull(hadoopConfiguration);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    org.apache.hadoop.fs.FileStatus hadoopStatus = ((HadoopFileStatus)file).getInternalFileStatus();
    org.apache.hadoop.fs.BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(hadoopStatus, start, len);
    HadoopBlockLocation[] distBlkLocations = new HadoopBlockLocation[blkLocations.length];

    for(int i = 0; i < distBlkLocations.length; ++i) {
      distBlkLocations[i] = new HadoopBlockLocation(blkLocations[i]);
    }

    return distBlkLocations;
  }

  @Override
  public RecoverableWriter createRecoverableWriter() {
    return new GooseFSBaseRecoverableWriter(fileSystem, hadoopConfiguration.getBoolean(Constants.FILESYSTEM_SUPPORT_RESUME_KEY, true));
  }

}
