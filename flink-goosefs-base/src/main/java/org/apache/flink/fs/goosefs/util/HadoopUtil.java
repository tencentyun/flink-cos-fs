package org.apache.flink.fs.goosefs.util;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Hadoop 工具类，提供 Hadoop 路径转换和临时文件生成功能。
 */
public class HadoopUtil {

  public static org.apache.hadoop.fs.Path toHadoopPath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toUri());
  }

  public static org.apache.hadoop.fs.Path generateStagingTempFilePath(
      FileSystem fs,
      org.apache.hadoop.fs.Path targetFile) throws IOException {

    checkArgument(targetFile.isAbsolute(), "targetFile must be absolute");

    final org.apache.hadoop.fs.Path parent = targetFile.getParent();
    final String name = targetFile.getName();

    checkArgument(parent != null, "targetFile must not be the root directory");

    while (true) {
      org.apache.hadoop.fs.Path candidate = new org.apache.hadoop.fs.Path(
          parent, "." + name + ".inprogress." + UUID.randomUUID());
      if (!fs.exists(candidate)) {
        return candidate;
      }
    }
  }
}


