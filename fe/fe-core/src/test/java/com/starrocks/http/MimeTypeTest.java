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

package com.starrocks.http;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.activation.MimetypesFileTypeMap;

public class MimeTypeTest {

    @Test
    public void test() {
        MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
        mimeTypesMap.addMimeTypes("text/html html htm");
        mimeTypesMap.addMimeTypes("application/javascript js");
        mimeTypesMap.addMimeTypes("text/css css");
        Assertions.assertEquals("text/css", mimeTypesMap.getContentType("/fe/webroot/static/datatables_bootstrap.css"));
        Assertions.assertEquals("application/javascript", mimeTypesMap.getContentType("/fe/webroot/static/web.js"));
        Assertions.assertEquals("text/html", mimeTypesMap.getContentType("/fe/webroot/index.html"));
        Assertions.assertEquals("text/html", mimeTypesMap.getContentType("/fe/webroot/index.htm"));
    }
}
