/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.processors.tekst;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

public class DownloadMultipleS3FilesByPrefixTest {

    @Test
    public void testAddingRequiredProperties() {
        TestRunner runner = TestRunners.newTestRunner(DownloadMultipleS3FilesByPrefix.class);

        DownloadMultipleS3FilesByPrefix processor = new DownloadMultipleS3FilesByPrefix();

        runner.setProperty(DownloadMultipleS3FilesByPrefix.BUCKET, "test-bucket");
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ACCESS_KEY, "access");
        runner.setProperty(DownloadMultipleS3FilesByPrefix.SECRET_KEY, "secret");
        runner.setProperty(DownloadMultipleS3FilesByPrefix.REGION, "eu-west-2");
        runner.setProperty(DownloadMultipleS3FilesByPrefix.PREFIX, "test-folder");
        runner.setProperty(DownloadMultipleS3FilesByPrefix.ENDPOINT, "http://localhost:8080");
        runner.setProperty(DownloadMultipleS3FilesByPrefix.LOCAL_FOLDER, "folder2");
    }

}
