package com.qcloud.cos.goosefs.hadoop;

import com.qcloud.cos.goosefs.GooseFSURI;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FlinkFileSinkGooseFSFileSystem {
  private final AbstractCompatibleFileSystem gooseFSFileSystem;

  public FlinkFileSinkGooseFSFileSystem(AbstractCompatibleFileSystem gooseFSFileSystem) {
    this.gooseFSFileSystem = gooseFSFileSystem;
  }

  public boolean isAccessUnderFileSystem(Path path) throws IOException {
    return gooseFSFileSystem.isAccessUnderFileSystem(path);
  }

  public GooseFSURI getGooseFSPath(Path path) throws IOException {
    return gooseFSFileSystem.getGooseFSPath(path);
  }
}
