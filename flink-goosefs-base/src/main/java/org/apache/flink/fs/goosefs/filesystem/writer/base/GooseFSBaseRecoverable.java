package org.apache.flink.fs.goosefs.filesystem.writer.base;

import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;
import org.apache.hadoop.fs.Path;

public class GooseFSBaseRecoverable implements CommitRecoverable, ResumeRecoverable {
  /** The file path for the final result file. */
  private final Path targetFile;

  /** The file path of the staging file. */
  private final Path tempFile;

  /** The position to resume from. */
  private final long offset;

  public GooseFSBaseRecoverable(Path targetFile, Path tempFile, long offset) {
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
    return "GooseFSBaseRecoverable " + tempFile + " @ " + offset + " -> " + targetFile;
  }
}
