// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/plugin/PluginZip.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.plugin;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.Util;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Describe plugin install file(.zip)
 * Support remote(http/https) source and local source
 */
class PluginZip {
    private static final Logger LOG = LogManager.getLogger(PluginZip.class);

    private static final List<String> DEFAULT_PROTOCOL = ImmutableList.of("https://", "http://");

    // timeout for both connection and read. 10 seconds is long enough.
    private static final int HTTP_TIMEOUT_MS = 10000;

    private String source;

    private List<Path> cleanPathList;

    private String expectedChecksum;

    public PluginZip(String source, String expectedMd5sum) {
        this.source = source;
        cleanPathList = Lists.newLinkedList();
        this.expectedChecksum = expectedMd5sum;
    }

    /*
     * download and extract the zip file to the target path.
     * return the path dir which contains all extracted files.
     */
    public Path extract(Path targetPath) throws IOException, StarRocksException {
        try {
            Path zipPath = downloadZip(targetPath);
            return extractZip(zipPath, targetPath);
        } finally {
            // clean temp path;
            for (Path p : cleanPathList) {
                FileUtils.deleteQuietly(p.toFile());
            }
        }
    }

    /**
     * download zip if the source in remote,
     * return the zip file path.
     * This zip file is currently in a temp directory, such ash
     **/
    Path downloadZip(Path targetPath) throws IOException, StarRocksException {
        if (Strings.isNullOrEmpty(source)) {
            throw new PluginException("empty plugin source path: " + source);
        }

        boolean isLocal = true;
        for (String p : DEFAULT_PROTOCOL) {
            if (StringUtils.startsWithIgnoreCase(StringUtils.trim(source), p)) {
                isLocal = false;
                break;
            }
        }

        if (!isLocal) {
            return downloadRemoteZip(targetPath);
        } else {
            return FileSystems.getDefault().getPath(source);
        }
    }

    /**
     * download zip and check md5
     **/
    Path downloadRemoteZip(Path targetPath) throws IOException, StarRocksException {
        LOG.info("download plugin zip from: " + source);

        Path zip = Files.createTempFile(targetPath, ".plugin_", ".zip");
        cleanPathList.add(zip);

        // download zip
        try (InputStream in = getInputStreamFromUrl(source)) {
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }

        // .md5 check
        if (Strings.isNullOrEmpty(expectedChecksum)) {
            try (InputStream in = getInputStreamFromUrl(source + ".md5")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                expectedChecksum = br.readLine();
            } catch (IOException e) {
                throw new StarRocksException(e.getMessage() +
                        ". you should set md5sum in plugin properties or provide a md5 URI to check plugin file");
            }
        }

        DigestUtils.md5Hex(Files.readAllBytes(zip));
        final String actualChecksum = DigestUtils.md5Hex(Files.readAllBytes(zip));

        if (!StringUtils.equalsIgnoreCase(expectedChecksum, actualChecksum)) {
            throw new StarRocksException(
                    "MD5 check mismatch, expected " + expectedChecksum + " but actual " + actualChecksum);
        }

        return zip;
    }

    /**
     * if `zipOrPath` is a zip file, unzip the specified .zip file to the targetPath.
     * if `zipOrPath` is a dir, copy the dir and its content to targetPath.
     */
    Path extractZip(Path zip, Path targetPath) throws IOException, StarRocksException {
        if (!Files.exists(zip)) {
            throw new PluginException("Download plugin zip failed. zip file does not exist. source: " + source);
        }

        if (Files.isDirectory(zip)) {
            // user install the plugin by dir/, so just copy the dir to the target path
            FileUtils.copyDirectory(zip.toFile(), targetPath.toFile());
            return targetPath;
        }

        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                Path targetFile = targetPath.resolve(entry.getName());
                if (entry.getName().startsWith("starrocks/")) {
                    throw new StarRocksException("Not use \"starrocks\" directory within the plugin zip.");
                }
                // Using the entry name as a path can result in an entry outside of the plugin dir,
                // either if the name starts with the root of the filesystem, or it is a relative
                // entry like ../whatever. This check attempts to identify both cases by first
                // normalizing the path (which removes foo/..) and ensuring the normalized entry
                // is still rooted with the target plugin directory.
                if (!targetFile.normalize().startsWith(targetPath)) {
                    throw new StarRocksException("Zip contains entry name '" +
                            entry.getName() + "' resolving outside of plugin directory");
                }

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                if (!Files.isSymbolicLink(targetFile.getParent())) {
                    Files.createDirectories(targetFile.getParent());
                }
                if (!entry.isDirectory()) {
                    try (OutputStream out = Files.newOutputStream(targetFile)) {
                        int len;
                        while ((len = zipInput.read(buffer)) >= 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipInput.closeEntry();
            }
        }

        return targetPath;
    }

    InputStream getInputStreamFromUrl(String url) throws IOException {
        return Util.getInputStreamFromUrl(url, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS);
    }
}
