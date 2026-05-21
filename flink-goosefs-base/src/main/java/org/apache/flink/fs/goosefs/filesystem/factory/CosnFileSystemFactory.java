package org.apache.flink.fs.goosefs.filesystem.factory;

public class CosnFileSystemFactory extends AbstractGooseFSFileSystemFactory {

  private static final String SCHEMA = "cosn";

  @Override
  public String getScheme() {
    return SCHEMA;
  }
}
