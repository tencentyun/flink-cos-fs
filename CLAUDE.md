# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Flink `FileSystem` plugin for Tencent Cloud COS (scheme `cosn://`), built on top of the `hadoop-cos` (CosN) Hadoop filesystem. Its main value over plain Hadoop wrapping is a `RecoverableWriter` implementation so `StreamingFileSink` can write to COS with exactly-once semantics. Targets Flink 1.10 / Java 8; the root POM's parent is `org.apache.flink:flink-filesystems:1.10.0`. Project version format is `${flink.version}-${version}` (e.g. `1.10.0-0.1.10`). The README is in Chinese and documents user-facing `fs.cosn.*` config keys.

## Build

```bash
mvn clean package -DskipTests                       # default: hadoop 3.1.0
mvn clean package -DskipTests -Dfs.hadoopshaded.version=3.x.x
mvn clean package -DskipTests -Phadoop-2 -Dfs.hadoopshaded.version=2.x.x
mvn clean package -DskipTests -pl flink-cos-fs-hadoop -am   # build just the plugin jar and its deps
```

- The deliverable is the shaded jar at `flink-cos-fs-hadoop/target/flink-cos-fs-hadoop-<version>.jar`, installed into `${FLINK_HOME}/plugins/cos-fs-hadoop/`.
- Changing the Hadoop version also requires manually editing the hardcoded version info in `flink-fs-hadoop-shaded/src/main/java/org/apache/hadoop/util/VersionInfo.java`.
- `maven-gpg-plugin` signing is bound to `verify`, so `mvn install`/`verify` needs GPG set up (or add `-Dgpg.skip`). `package` doesn't need it.
- There are no unit tests in the repo. `flink-cos-fs-examples` (`StreamingFileSinkTest`, `MockSource`) is a manual end-to-end job that needs a real COS bucket and credentials.
- Checkstyle/spotbugs configs from the Flink parent are expected under `tools/maven/`. The stray `${rootDir}/` directories inside each module are build artifacts caused by the parent POM's unresolved `rootDir` property; don't commit them.

## Module layout and dependency flow

```
flink-fs-hadoop-shaded  →  flink-cos-fs-base  →  flink-cos-fs-hadoop (shaded plugin jar)
                                               flink-cos-fs-examples (sample job)
```

- **flink-fs-hadoop-shaded**: Repackages `hadoop-common` (`hadoop-2` / `hadoop-3` profiles, with `hadoop-3` active by default) and overrides a few Hadoop classes (`Configuration`, `VersionInfo`, `NativeCodeLoader`, `StorageSize/Unit`) plus `core-default-shaded.xml`. This mirrors Flink's own `flink-fs-hadoop-shaded`.
- **flink-cos-fs-base** (`org.apache.flink.fs.cos.common`): All of the real logic. It depends on `hadoop-cos` (`${fs.hadoopshaded.version}-${fs.cosn.version}`), `cos_api-bundle`, and the CHDFS/OFS network plugin. The SDK versions are set as properties in this module's POM.
- **flink-cos-fs-hadoop** (`org.apache.flink.fs.coshadoop`): The concrete `COSNFileSystemFactory` (registered via `META-INF/services/org.apache.flink.core.fs.FileSystemFactory`) and `HadoopCOSAccessHelper`, which adapts CosN's `NativeFileSystemStore` to the `COSAccessHelper` interface. It also produces the shaded uber jar.

## Architecture

**Filesystem creation** (`AbstractCOSFileSystemFactory.create`): it copies every Flink config key prefixed with `fs.cosn.` or `fs.AbstractFileSystem.cosn.` into a Hadoop `Configuration` (in `COSNFileSystemFactory.getHadoopConfiguration`), initializes a `CosFileSystem`, and wraps it in `FlinkCOSFileSystem` (extends Flink's `HadoopFileSystem`, kind `OBJECT_STORE`). Flink-side options are `cos.upload.part.min.size`, `cos.upload.max.concurrent.uploads`, and `cos.recover.wait.time.seconds`.

**Two RecoverableWriter implementations.** `FlinkCOSFileSystem.createRecoverableWriter` picks one based on whether `CosNConfigKeys.COSN_POSIX_BUCKET_FS_IMPL` equals the default posix-bucket impl:

1. **Object bucket, multipart upload (`writer/`)**: This is a port of Flink's S3 recoverable writer. `COSRecoverableWriter` → `COSRecoverableFsDataOutputStream` buffers data into local ref-counted temp files (`utils/RefCounted*`), and `RecoverableMultipartUploadImpl` uploads parts asynchronously (bounded by `BackPressuringExecutor`). `COSRecoverable` holds the upload ID, part ETags, and the in-progress tail object. `COSCommitter` completes the MPU. On creation, `COSRecoverableWriter.writer()` sleeps until `initTimestamp + cos.recover.wait.time.seconds` so in-flight part uploads from before a failover finish first.
2. **POSIX / merge bucket (CHDFS-backed) (`fswriter/`)**: This is a port of Flink's HDFS `HadoopRecoverableWriter`. It writes to a staging temp file and uses `hsync` for persist. Recovery truncates the file (via reflection on `FileSystem.truncate`) and then appends. The commit is a rename (`COSMergeCommitter`). Before truncating it has to release the file's fd lock explicitly. Leftover "merge" naming refers to this mode.

Each writer has its own `SimpleVersionedSerializer` (`COSRecoverableSerializer`, `COSPosixRecoverableSerializer`). Changing either serialized format breaks restoring from existing checkpoints unless you bump the serializer version and handle the old one.

**Shading (`flink-cos-fs-hadoop/pom.xml`)**:
- `org.apache.commons` and `com.google.common` are relocated to `org.apache.flink.fs.shaded.hadoop3.*`. `com.qcloud` is relocated to `org.apache.flink.fs.coshadoop.shaded.com.qcloud`, except `com.qcloud.chdfs.*`. Flink's `runtime.fs.hdfs` / `runtime.util` are relocated to `org.apache.flink.fs.coshadoop.common`.
- `org.apache.hadoop` is deliberately **not** relocated right now. The merge-bucket network plugin loads Hadoop classes by their real names, so relocating them causes `ClassNotFoundException`. The downside is that the plugin's Hadoop must not conflict with the cluster's.
- `COSNFileSystemFactory` still rewrites `fs.cosn.credentials.provider` values that start with `org.apache.hadoop.fs` to the `org.apache.flink.fs.shaded.hadoop3.` prefix. Keep this consistent with whatever relocation is in effect.
- `maven-shade-plugin` is pinned to 3.2.4 for compatibility with `cos_api-bundle`.
