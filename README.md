# Flink-cos-fs

Flink-cos-fs 是腾讯云对象存储系统COS针对Flink的文件系统实现，并且支持了recoverwriter接口。 Flink可以基于该文件系统实现读写COS上的数据以及作为流应用的状态后端。

## 使用环境

### 系统环境

支持Linux和Mac OS系统

### 软件依赖

Flink 1.10


## 使用方法

### 获取Flink-cos-fs 发行包

下载地址：[Flink-cos-fs release](https://github.com/yuyang733/flink-cos-fs/releases)


### 安装Flink-cos-fs依赖

1.执行`mkdir ${FLINK_HOME}/plugins/cos-fs-hadoop`，在`${FLINK_HOME}/plugins`目录下创建flink-cos-fs-hadoop插件目录；

2.将对应版本的预编译包（flink-cos-fs-{flink.version}-{version}.jar）拷贝到`${FLINK_HOME}/plugins/cos-fs-hadoop`目录下；

3.在${FLINK_HOME}/conf/flink-conf.yaml中添加一些COSN相关配置以确保flink能够访问到COS存储桶，这里的配置键与COSN完全兼容，可参考[hadoop-cos:[对象存储 Hadoop 工具 - 工具指南 - 文档中心 - 腾讯云](https://cloud.tencent.com/document/product/436/6884)](https://cloud.tencent.com/document/product/436/6884)，必须配置信息如下：

```yaml
fs.cosn.impl: org.apache.hadoop.fs.CosFileSystem
fs.AbstractFileSystem.cosn.impl: org.apache.hadoop.fs.CosN
fs.cosn.userinfo.secretId: AKIDXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
fs.cosn.userinfo.secretKey: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
fs.cosn.bucket.region: ap-guangzhou
fs.cosn.bucket.endpoint_suffix: cos.ap-guangzhou.myqcloud.com

```

4.在作业的write或sink路径中填写格式为：```cosn://bucket-appid/path```的路径信息即可，例如：

```java
        ...
        StreamingFileSink<String> fileSink  =  StreamingFileSink.forRowFormat(
                new Path("cosn://flink-test-1250000000/sink-test"),
                new SimpleStringEncoder<String>("UTF-8"))
                .build();
        ...
```

### 使用示例

以下给出Flink Job读写COS的示例代码：

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
