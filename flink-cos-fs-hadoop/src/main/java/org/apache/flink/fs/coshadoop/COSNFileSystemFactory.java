/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.coshadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.cos.common.AbstractCOSFileSystemFactory;
import org.apache.flink.fs.cos.common.writer.COSAccessHelper;

import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.NativeFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

/** The CosN FileSystem Factory. */
public class COSNFileSystemFactory extends AbstractCOSFileSystemFactory {
    private static final Logger LOG = LoggerFactory.getLogger(COSNFileSystemFactory.class);

    private Configuration flinkConfig;

    private static final Set<String> CONFIG_KEYS_TO_SHADE =
            Collections.singleton("fs.cosn.credentials.provider");

    private static final String FLINK_SHADING_PREFIX = "org.apache.flink.fs.shaded.hadoop3.";

    /**
     * In order to simplify, we make flink cos configuration keys same with hadoop cos module. So,
     * we add all configuration key with prefix `fs.cosn` in flink conf to hadoop conf
     */
    private static final String[] FLINK_CONFIG_PREFIXES = {
        "fs.cosn.", "fs.AbstractFileSystem.cosn."
    };

    public COSNFileSystemFactory() {
        super("COSN FileSystem");
    }

    @Override
    protected FileSystem createHadoopFileSystem() {
        return new CosFileSystem();
    }

    @Override
    protected URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
        final String scheme = fsUri.getScheme();
        final String authority = fsUri.getAuthority();

        if (scheme == null && authority == null) {
            fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
        } else if (scheme != null && authority == null) {
            URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
            if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                fsUri = defaultUri;
            }
        }

        return fsUri;
    }

    @Override
    protected COSAccessHelper getCosAccessHelper(NativeFileSystemStore nativeFileSystemStore) {
        return new HadoopCOSAccessHelper(nativeFileSystemStore);
    }

    @Override
    public void configure(org.apache.flink.configuration.Configuration config) {
        super.configure(config);
        this.flinkConfig = config;
    }

    @Override
    protected org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flinkConfig == null) {
            return conf;
        }

        // read all configuration with prefix 'FLINK_CONFIG_PREFIXES'
        for (String key : flinkConfig.keySet()) {
            for (String prefix : FLINK_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = flinkConfig.getString(key, null);
                    conf.set(key, value);
                    if (CONFIG_KEYS_TO_SHADE.contains(key)) {
                        if (value.startsWith("org.apache.hadoop.fs")) {
                            conf.set(key, FLINK_SHADING_PREFIX + value);
                        }
                    }
                    LOG.debug(
                            "Adding Flink config entry for {} as {} to Hadoop config",
                            key,
                            conf.get(key));
                }
            }
        }
        return conf;
    }

    @Override
    public String getScheme() {
        return "cosn";
    }
}
