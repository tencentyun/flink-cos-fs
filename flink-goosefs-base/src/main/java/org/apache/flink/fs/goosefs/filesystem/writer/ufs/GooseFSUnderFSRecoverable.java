package org.apache.flink.fs.goosefs.filesystem.writer.ufs;

import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;

import org.apache.hadoop.fs.Path;

/**
 * GooseFS 底层文件系统可恢复写入状态类。
 * 实现了 CommitRecoverable 和 ResumeRecoverable 接口，用于支持可恢复的文件写入操作。
 */
public class GooseFSUnderFSRecoverable implements CommitRecoverable, ResumeRecoverable {
  /** The file path for the final result file. */
  private final Path targetFile;

  /** The file path of the staging file. */
  private final Path tempFile;

  /** The position to resume from. */
  private final long offset;

  public GooseFSUnderFSRecoverable(Path targetFile, Path tempFile, long offset) {
    this.targetFile = targetFile;
    this.tempFile = tempFile;
    this.offset = offset;
  }

  public Path targetFile() {
    return targetFile;
  }

  public Path tempFile() {
    return tempFile;
  }

  public long offset() {
    return offset;
  }

  @Override
  public String toString() {
    return "GooseFSUnderFSRecoverable " + tempFile + " @ " + offset + " -> " + targetFile;
  }
}
