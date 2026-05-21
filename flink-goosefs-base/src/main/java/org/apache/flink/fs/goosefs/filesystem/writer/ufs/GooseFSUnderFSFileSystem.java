package org.apache.flink.fs.goosefs.filesystem.writer.ufs;

import com.qcloud.cos.goosefs.ClientContext;
import com.qcloud.cos.goosefs.conf.GooseFSProperties;
import com.qcloud.cos.goosefs.conf.InstancedConfiguration;
import com.qcloud.cos.goosefs.conf.Source;
import com.qcloud.cos.goosefs.hadoop.AbstractCompatibleFileSystem;
import com.qcloud.cos.goosefs.hadoop.CosNFileSystem;
import com.qcloud.cos.goosefs.hadoop.FlinkFileSinkGooseFSFileSystem;
import com.qcloud.cos.goosefs.hadoop.HadoopConfigurationUtils;
import com.qcloud.cos.goosefs.util.ConfigurationUtils;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class GooseFSUnderFSFileSystem extends HadoopFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(GooseFSUnderFSFileSystem.class);

  private FlinkFileSinkGooseFSFileSystem flinkFileSinkGooseFSFileSystem;
  private org.apache.hadoop.conf.Configuration hadoopConfiguration;
  private com.qcloud.cos.goosefs.client.file.FileSystem gooseFSFileSystem;

  public GooseFSUnderFSFileSystem(CosNFileSystem fileSystem, org.apache.hadoop.conf.Configuration hadoopConfiguration) throws IOException {
    super(fileSystem);
    this.hadoopConfiguration = checkNotNull(hadoopConfiguration);
    initGooseFSFileSystem();

    if (fileSystem instanceof AbstractCompatibleFileSystem) {
      this.flinkFileSinkGooseFSFileSystem = new FlinkFileSinkGooseFSFileSystem((AbstractCompatibleFileSystem) fileSystem);
    } else {
      throw new IOException(String.format("unsupported fileSystem [%s]", fileSystem));
    }
  }

  private void initGooseFSFileSystem() {
    LOG.info("init GooseFS FileSystem");
    GooseFSProperties goosefsProps =  ConfigurationUtils.defaults();
    goosefsProps.merge(HadoopConfigurationUtils.getConfigurationFromHadoop(hadoopConfiguration), Source.RUNTIME);
    InstancedConfiguration instancedConfiguration = new InstancedConfiguration(goosefsProps);
    instancedConfiguration.validate();
    this.gooseFSFileSystem = com.qcloud.cos.goosefs.client.file.FileSystem.Factory.create(
        ClientContext.create(new Subject(), instancedConfiguration).setUriValidationEnabled(true));
  }

  @Override
  public RecoverableWriter createRecoverableWriter() {
    return new GooseFSUnderFSRecoverableWriter(flinkFileSinkGooseFSFileSystem, gooseFSFileSystem, hadoopConfiguration);
  }
}
