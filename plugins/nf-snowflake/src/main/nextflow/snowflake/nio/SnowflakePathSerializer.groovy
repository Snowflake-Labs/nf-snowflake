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

package nextflow.snowflake.nio

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.util.SerializerRegistrant
import org.pf4j.Extension

import java.nio.file.spi.FileSystemProvider

/**
 * Register the SnowflakePath serializer
 *
 * @author Hongye Yu
 */
@Slf4j
@Extension
@CompileStatic
class SnowflakePathSerializer extends Serializer<SnowflakePath> implements SerializerRegistrant {

    @Override
    void register(Map<Class, Object> serializers) {
        serializers.put(SnowflakePath, SnowflakePathSerializer)
    }

    @Override
    void write(Kryo kryo, Output output, SnowflakePath target) {
        final scheme = target.getFileSystem().provider().getScheme()
        final path = target.toString()
        log.trace "SnowflakePath serialization > scheme: $scheme; path: $path"
        output.writeString(scheme)
        output.writeString(path)
    }

    @Override
    SnowflakePath read(Kryo kryo, Input input, Class<SnowflakePath> type) {
        final scheme = input.readString()
        final path = input.readString()
        if (scheme != 'snowflake') {
            throw new IllegalStateException("Unexpected scheme for Snowflake path -- offending value '$scheme'")
        }
        log.trace "SnowflakePath de-serialization > scheme: $scheme; path: $path"

        // Reconstruct the URI and use the provider to get the path
        final uri = "snowflake://stage/${path}"

        // Find the Snowflake FileSystemProvider
        SnowflakeFileSystemProvider provider = FileSystemProvider.installedProviders()
            .find { it instanceof SnowflakeFileSystemProvider } as SnowflakeFileSystemProvider

        if (!provider) {
            throw new IllegalStateException("SnowflakeFileSystemProvider not found in installed providers")
        }

        return (SnowflakePath) provider.getPath(new URI(uri))
    }
}
