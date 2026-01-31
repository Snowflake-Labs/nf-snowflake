/*
 * Copyright 2021, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.plugin.BasePlugin
import nextflow.snowflake.nio.SnowflakeFileSystemProvider
import org.pf4j.PluginWrapper

/**
 * Nextflow plugin for Snowflake extensions
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakePlugin extends BasePlugin {

    SnowflakePlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    @Override
    void start() {
        super.start()
        log.debug("Starting Snowflake plugin - registering FileSystemProvider")
        // Register the Snowflake FileSystemProvider
        FileHelper.getOrInstallProvider(SnowflakeFileSystemProvider)
        log.info("Snowflake FileSystemProvider registered successfully")
    }
}
