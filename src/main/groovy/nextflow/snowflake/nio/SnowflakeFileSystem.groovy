package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

/**
 * Implements a FileSystem for Snowflake stages
 * 
 * Manages the connection to Snowflake and creates SnowflakePath instances
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeFileSystem extends FileSystem {

    private final SnowflakeFileSystemProvider provider
    private final SnowflakeStageClient client
    private volatile boolean open = true

    SnowflakeFileSystem(SnowflakeFileSystemProvider provider, SnowflakeStageClient client) {
        this.provider = provider
        this.client = client
    }

    SnowflakeStageClient getClient() {
        return client
    }

    @Override
    FileSystemProvider provider() {
        return provider
    }

    @Override
    void close() throws IOException {
        if (open) {
            open = false
            log.debug("Snowflake filesystem closed")
        }
    }

    @Override
    boolean isOpen() {
        return open
    }

    @Override
    boolean isReadOnly() {
        return false
    }

    @Override
    String getSeparator() {
        return '/'
    }

    @Override
    Iterable<Path> getRootDirectories() {
        // Snowflake stages don't have a traditional root directory concept
        // Return empty list as roots are defined by stage names
        return Collections.emptyList()
    }

    @Override
    Iterable<FileStore> getFileStores() {
        // Snowflake stages don't expose file store information
        return Collections.emptyList()
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return Collections.singleton('basic')
    }

    @Override
    Path getPath(String first, String... more) {
        if (more.length == 0) {
            return SnowflakePath.parse(this, first)
        }
        
        // Combine path segments
        StringBuilder sb = new StringBuilder(first)
        for (String segment : more) {
            if (segment) {
                if (!sb.toString().endsWith('/')) {
                    sb.append('/')
                }
                sb.append(segment)
            }
        }
        
        return SnowflakePath.parse(this, sb.toString())
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException("Path matchers not supported for Snowflake filesystem")
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("User principal lookup not supported for Snowflake filesystem")
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Watch service not supported for Snowflake filesystem")
    }

    @Override
    String toString() {
        return "SnowflakeFileSystem"
    }
}

