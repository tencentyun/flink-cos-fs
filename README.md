# Flink-cos-fs

Flink-cos-fs 是腾讯云对象存储系统COS针对 Flink 的文件系统实现，并且支持了 recoverwriter 接口。 Flink 可以基于该文件系统实现读写 COS 上的数据以及作为流应用的状态后端。

## 使用环境

### 系统环境

支持 Linux 和 Mac OS 系统

### 软件依赖

Flink 1.10


## 使用方法

### 编译或获取 Flink-cos-fs 发行包

#### 编译 flink-cos-fs

flink-cos-fs 默认依赖的是 hadoop-3.1.0 的版本（即是于 flink-1.10 保持一致），使用如下命令即可编译打包：

```bash
mvn clean package -DskipTests
```

如果需要编译依赖 hadoop 其他版本的发行包，则需要手动修改项目根路径 pom.xml 中的 `${fs.hadoopshaded.version}` 或者在编译命令中指定 `-Dfs.hadoopshaded.version=3.x.x`，同时修改 flink-fs-hadoop-shaded 模块下的 org.apache.hadoop.util.VersionInfo.java 文件中的 hadoop 版本信息：

```java
// ...
		if ("common".equals(component)) {
			info.setProperty("version", "3.1.0");
			info.setProperty("revision", "16b70619a24cdcf5d3b0fcf4b58ca77238ccbe6d");
			info.setProperty("branch", "branch-3.1.0");
			info.setProperty("user", "wtan");
			info.setProperty("date", "2018-04-03T04:00Z");
			info.setProperty("url", "git@github.com:hortonworks/hadoop-common-trunk.git");
			info.setProperty("srcChecksum", "14182d20c972b3e2105580a1ad6990");
			info.setProperty("protocVersion", "2.5.0");
		}
// ...
```

特别地，如果需要编译 hadoop 2.x 的版本，除了上述操作以外，还需要在编译命令中，指定 maven 编译使用 `hadoop-2` 的配置：
```bash
mvn clean package -DskipTests -Phadoop-2 -Dfs.hadoopshaded.version=2.x.x
```

最后，编译完成以后，在 `${FLINK_COS_FS}/flink-cos-fs-hadoop/target` 就可以得到 `flink-cos-fs-hadoop-${flink.version}-{version}.jar` 的依赖包。

下载地址：[Flink-cos-fs release](https://github.com/yuyang733/flink-cos-fs/releases)


### 安装Flink-cos-fs依赖

1.执行`mkdir ${FLINK_HOME}/plugins/cos-fs-hadoop`，在`${FLINK_HOME}/plugins`目录下创建flink-cos-fs-hadoop插件目录；

2.将对应版本的预编译包（flink-cos-fs-{flink.version}-{version}.jar）拷贝到`${FLINK_HOME}/plugins/cos-fs-hadoop`目录下；

3.在 `${FLINK_HOME}/conf/flink-conf.yaml` 中添加一些 CosN 相关配置以确保 Flink 能够访问到 COS 存储桶，这里的配置键与 CosN 完全兼容，可参考[hadoop-cos](https://cloud.tencent.com/document/product/436/6884)，必须配置信息如下：

```yaml
fs.cosn.impl: org.apache.hadoop.fs.CosFileSystem
fs.AbstractFileSystem.cosn.impl: org.apache.hadoop.fs.CosN
fs.cosn.userinfo.secretId: AKIDXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
fs.cosn.userinfo.secretKey: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
fs.cosn.bucket.region: ap-guangzhou
fs.cosn.bucket.endpoint_suffix: cos.ap-guangzhou.myqcloud.com

```

4.在作业的 write 或 sink 路径中填写格式为：```cosn://bucket-appid/path```的路径信息即可，例如：

```java
        ...
        StreamingFileSink<String> fileSink  =  StreamingFileSink.forRowFormat(
                new Path("cosn://flink-test-1250000000/sink-test"),
                new SimpleStringEncoder<String>("UTF-8"))
                .build();
        ...
```

### 使用示例

以下给出 Flink Job 读写 COS 的示例代码：

```Java
// Read from COS bucket
env.readTextFile("cosn://<bucket-appid>/<object-name>");

// Write to COS bucket
stream.writeAsText("cosn://<bucket-appid>/<object-name>");

// Use COS as FsStatebackend
env.setStateBackend(new FsStateBackend("cosn://<bucket-appid>/<object-name>"));

// Use the streamingFileSink which supports the recoverable writer
StreamingFileSink<String> fileSink  =  StreamingFileSink.forRowFormat(
                new Path("cosn://<bucket-appid>/<object-name>"),new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(build).build();

```


## 所有配置说明

| 属性键                             | 说明                | 默认值 | 必填项 |
|:-----------------------------------:|:--------------------|:-----:|:---:|
|fs.cosn.bucket.endpoint_suffix|指定要连接的COS endpoint，该项为非必填项目。对于公有云COS用户而言，只需要正确填写上述的region配置即可。兼容原配置项：fs.cosn.userinfo.endpoint_suffix。|无|否|
|fs.cosn.userinfo.secretId/secretKey| 填写您账户的API 密钥信息。可通过 [云 API 密钥 控制台](https://console.cloud.tencent.com/capi) 查看。| 无  | 是|
|fs.cosn.impl                      | cosn对FileSystem的实现类，固定为 org.apache.hadoop.fs.CosFileSystem。| 无|是|
|fs.AbstractFileSystem.cosn.impl   | cosn对AbstractFileSy stem的实现类，固定为org.apache.hadoop.fs.CosN。| 无 |是|
|fs.cosn.bucket.region           | 请填写您的地域信息，枚举值为 [可用地域](https://cloud.tencent.com/document/product/436/6224) 中的地域简称，如ap-beijing、ap-guangzhou等。 兼容原配置项：fs.cosn.userinfo.region。| 无 | 是|
|fs.cosn.tmp.dir                   | 请设置一个实际存在的本地目录，运行过程中产生的临时文件会暂时放于此处。|/tmp/hadoop_cos | 否|
|fs.cosn.block.size                | CosN文件系统每个block的大小，默认为128MB | ‭134217728‬（128MB） | 否 |
|fs.cosn.upload.buffer             | CosN文件系统上传时依赖的缓冲区类型。当前支持三种类型的缓冲区：非直接内存缓冲区（non_direct_memory），直接内存缓冲区（direct_memory），磁盘映射缓冲区（mapped_disk）。非直接内存缓冲区使用的是JVM堆内存，直接内存缓冲区使用的是堆外内存，而磁盘映射缓冲区则是基于内存文件映射得到的缓冲区。| mapped_disk | 否 |
|fs.cosn.upload.buffer.size        | CosN文件系统上传时依赖的缓冲区大小，如果指定为-1，则表示不限制。若不限制缓冲区大小，则缓冲区类型必须为mapped_disk。如果指定大小大于0，则要求该值至少大于等于一个block的大小。兼容原配置项：fs.cosn.buffer.size。| 134217728（128MB）|否|
|fs.cosn.upload.part.size          | 分块上传时每个part的大小。由于 COS 的分块上传最多只能支持10000块，因此需要预估最大可能使用到的单文件大小。例如，part size 为8MB时，最大能够支持78GB的单文件上传。 part size 最大可以支持到2GB，即单文件最大可支持19TB。| 8388608（8MB）| 否 |
|fs.cosn.upload_thread_pool        | 文件流式上传到COS时，并发上传的线程数目 | 8 | 否|
|fs.cosn.copy_thread_pool 		   | 目录拷贝操作时，可用于并发拷贝和删除文件的线程数目 | 3 | 否 |
|fs.cosn.read.ahead.block.size     | 预读块的大小                                 | ‭1048576‬（1MB） |  否 |
|fs.cosn.read.ahead.queue.size     | 预读队列的长度                               | 8              | 否  |
|fs.cosn.maxRetries                | 访问COS出现错误时，最多重试的次数 | 200 | 否 |
|fs.cosn.retry.interval.seconds    | 每次重试的时间间隔 | 3 | 否 |
|fs.cosn.max.connection.num | 配置COS连接池中维持的最大连接数目，这个数目与单机读写COS的并发有关，建议至少大于或等于单机读写COS的并发数| 1024 | 否|
|fs.cosn.customer.domain | 配置COS的自定义域名，默认为空| 无 | 否|
|fs.cosn.server-side-encryption.algorithm | 配置COS服务端加密算法，支持SSE-C和SSE-COS，默认为空，不加密| 无 | 否|
|fs.cosn.server-side-encryption.key | 当开启COS的SSE-C服务端加密算法时，必须配置SSE-C的密钥，密钥格式为base64编码的AES-256密钥，默认为空，不加密| 无 | 否|
|fs.cosn.crc64.checksum.enabled    | 是否开启CRC64校验。默认不开启，此时无法使用`hadoop fs -checksum`命令获取文件的CRC64校验值。| false | 否 |
|fs.cosn.traffic.limit | 上传下载带宽的控制选项，819200 ~ 838860800，单位为bits/s。默认值为-1，表示不限制。 | -1 | 否 |


## FAQ

- Flink 既可以通过[hadoop-cos](https://github.com/tencentyun/hadoop-cos)读写 COS 中的对象文件，也可以通过 flink-cos-fs 来读写，这两种有什么区别？

hadoop-cos 实现了 Hadoop 的兼容文件系统语义，Flink 可以通过写 Hadoop 兼容文件系统的形式写入数据到 COS 中，但是这种方式不支持的 Flink 的 recoverable writer 写入，当你使用 streamingFileSink 写入数据时，要求底层文件系统支持recoverable writer。 因此，flink-cos-fs 基于 Hadoop-COS (CosN) 扩展实现了 Flink 的recoverable writer，完整地支持了 Flink 文件系统的语义，因此推荐使用它来访问 COS 对象。
