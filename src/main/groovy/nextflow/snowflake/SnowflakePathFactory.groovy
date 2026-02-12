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

import java.nio.file.Path
import java.nio.file.spi.FileSystemProvider

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileSystemPathFactory
import nextflow.snowflake.nio.SnowflakePath
import nextflow.snowflake.nio.SnowflakeFileSystemProvider

/**
 * Implements FileSystemPathFactory for Snowflake stages
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakePathFactory extends FileSystemPathFactory {

    @Override
    protected Path parseUri(String uri) {
        // Use centralized URI checking
        if (!SnowflakeUri.isSnowflakeStageUri(uri)) {
            return null
        }

        try {
            URI parsedUri = new URI(uri)

            // Find the Snowflake FileSystemProvider
            SnowflakeFileSystemProvider provider = FileSystemProvider.installedProviders()
                .find { it instanceof SnowflakeFileSystemProvider } as SnowflakeFileSystemProvider

            if (!provider) {
                log.warn("Snowflake FileSystemProvider not found")
                return null
            }

            return provider.getPath(parsedUri)
        }
        catch (Exception e) {
            log.warn("Failed to parse Snowflake URI: ${uri}", e)
            return null
        }
    }

    @Override
    protected String toUriString(Path path) {
        if (!(path instanceof SnowflakePath)) {
            return null
        }

        SnowflakePath snowPath = (SnowflakePath) path
        // Handle relative paths - return null to let default handler manage them
        if (!snowPath.isAbsolute()) {
            return null
        }
        return snowPath.toUri().toString()
    }

    @Override
    protected String getBashLib(Path path) {
        // No special bash library needed for Snowflake
        return null
    }

    @Override
    protected String getUploadCmd(String source, Path target) {
        if (!(target instanceof SnowflakePath)) {
            return null
        }

        SnowflakePath snowPath = (SnowflakePath) target
        String stageRef = snowPath.toStageReference()
        return "PUT file://${source} @${stageRef}"
    }
}
