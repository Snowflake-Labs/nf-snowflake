package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.snowflake.SnowflakeConnectionPool

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.ConcurrentHashMap

/**
 * FileSystemProvider implementation for Snowflake stages
 * 
 * Enables Nextflow to use Snowflake stages as file systems with the
 * snowflake://stage/<stage_name>/path URI scheme
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeFileSystemProvider extends FileSystemProvider {

    private static final String SCHEME = 'snowflake'
    
    private final Map<String, SnowflakeFileSystem> fileSystems = new ConcurrentHashMap<>()
    private final SnowflakeStageClient client

    SnowflakeFileSystemProvider() {
        this.client = new SnowflakeStageClient(SnowflakeConnectionPool.getInstance())
        log.info("SnowflakeFileSystemProvider initialized with scheme: ${getScheme()}")
    }

    @Override
    String getScheme() {
        return SCHEME
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        checkUri(uri)
        
        String key = getFileSystemKey(uri)
        
        if (fileSystems.containsKey(key)) {
            throw new FileSystemAlreadyExistsException("Filesystem already exists for: ${uri}")
        }
        
        SnowflakeFileSystem fileSystem = new SnowflakeFileSystem(this, client)
        fileSystems.put(key, fileSystem)
        
        log.debug("Created new Snowflake filesystem for key: ${key}")
        return fileSystem
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        checkUri(uri)
        
        String key = getFileSystemKey(uri)
        SnowflakeFileSystem fileSystem = fileSystems.get(key)
        
        if (fileSystem == null) {
            // Create a new filesystem if it doesn't exist
            try {
                return newFileSystem(uri, [:])
            } catch (FileSystemAlreadyExistsException e) {
                // Race condition - try to get it again
                fileSystem = fileSystems.get(key)
                if (fileSystem != null) {
                    return fileSystem
                }
                throw e
            }
        }
        
        return fileSystem
    }

    @Override
    Path getPath(URI uri) {
        checkUri(uri)
        
        FileSystem fileSystem = getFileSystem(uri)
        return SnowflakePath.parse((SnowflakeFileSystem) fileSystem, uri)
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        SnowflakePath snowflakePath = toSnowflakePath(path)
        return new SnowflakeStageInputStream(client, snowflakePath)
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        SnowflakePath snowflakePath = toSnowflakePath(path)
        
        // Check if we should fail if file exists
        boolean append = false
        boolean truncate = true
        for (OpenOption option : options) {
            if (option == StandardOpenOption.CREATE_NEW) {
                if (client.exists(snowflakePath.toStageReference())) {
                    throw new FileAlreadyExistsException(path.toString())
                }
            } else if (option == StandardOpenOption.APPEND) {
                append = true
                truncate = false
            }
        }
        
        return new SnowflakeStageOutputStream(client, snowflakePath)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("Seekable byte channels not supported for Snowflake paths")
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        SnowflakePath snowflakePath = toSnowflakePath(dir)
        
        String stagePath = snowflakePath.toStageReference()
        List<SnowflakeFileAttributes> files = client.list(stagePath)
        
        List<Path> paths = files.collect { SnowflakeFileAttributes attrs ->
            // Extract relative path from full stage path
            String fullPath = attrs.name
            (Path) SnowflakePath.parse(snowflakePath.fileSystem as SnowflakeFileSystem, "snowflake://stage/${fullPath}" as String)
        }.findAll { Path p -> filter.accept(p) } as List<Path>
        
        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                return paths.iterator()
            }
            
            @Override
            void close() throws IOException {
                // Nothing to close
            }
        }
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        // Snowflake stages don't require explicit directory creation
        // Directories are implicit in the path structure
        log.debug("Directory creation is implicit in Snowflake stages: ${dir}")
    }

    @Override
    void delete(Path path) throws IOException {
        SnowflakePath snowflakePath = toSnowflakePath(path)
        client.delete(snowflakePath.toStageReference())
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        SnowflakePath sourcePath = toSnowflakePath(source)
        SnowflakePath targetPath = toSnowflakePath(target)
        
        // Check if target exists and handle replace option
        boolean replaceExisting = false
        for (CopyOption option : options) {
            if (option == StandardCopyOption.REPLACE_EXISTING) {
                replaceExisting = true
            }
        }
        
        if (!replaceExisting && client.exists(targetPath.toStageReference())) {
            throw new FileAlreadyExistsException(target.toString())
        }
        
        // Download from source and upload to target
        ByteArrayOutputStream buffer = new ByteArrayOutputStream()
        client.download(sourcePath.toStageReference(), buffer)
        
        ByteArrayInputStream input = new ByteArrayInputStream(buffer.toByteArray())
        client.upload(targetPath.toStageReference(), input, buffer.size())
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        // Snowflake doesn't have a native move operation
        // Implement as copy + delete
        copy(source, target, options)
        delete(source)
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        return path.toAbsolutePath() == path2.toAbsolutePath()
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        // Snowflake stage files are not hidden
        return false
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException("File stores not supported for Snowflake paths")
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        SnowflakePath snowflakePath = toSnowflakePath(path)
        
        // Check if file exists
        if (!client.exists(snowflakePath.toStageReference())) {
            throw new NoSuchFileException(path.toString())
        }
        
        // Snowflake stages support read and write access
        // No way to check specific permissions via JDBC
    }

    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        throw new UnsupportedOperationException("File attribute views not supported for Snowflake paths")
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if (type != BasicFileAttributes.class) {
            throw new UnsupportedOperationException("Only BasicFileAttributes supported for Snowflake paths")
        }
        
        SnowflakePath snowflakePath = toSnowflakePath(path)
        SnowflakeFileAttributes attrs = client.getMetadata(snowflakePath.toStageReference())
        
        return (A) attrs
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        SnowflakePath snowflakePath = toSnowflakePath(path)
        SnowflakeFileAttributes attrs = client.getMetadata(snowflakePath.toStageReference())
        
        Map<String, Object> result = [:]
        
        if (attributes == '*' || attributes.contains('size')) {
            result.put('size', attrs.size())
        }
        if (attributes == '*' || attributes.contains('lastModifiedTime')) {
            result.put('lastModifiedTime', attrs.lastModifiedTime())
        }
        if (attributes == '*' || attributes.contains('isDirectory')) {
            result.put('isDirectory', attrs.isDirectory())
        }
        if (attributes == '*' || attributes.contains('isRegularFile')) {
            result.put('isRegularFile', attrs.isRegularFile())
        }
        
        return result
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Setting attributes not supported for Snowflake paths")
    }

    private void checkUri(URI uri) {
        if (uri.scheme != SCHEME) {
            throw new IllegalArgumentException("Invalid URI scheme: ${uri.scheme}. Expected '${SCHEME}'")
        }
        
        if (uri.authority != 'stage') {
            throw new IllegalArgumentException("Invalid URI authority: ${uri.authority}. Expected 'stage'")
        }
    }

    private String getFileSystemKey(URI uri) {
        // Use a simple key since all Snowflake stages use the same connection
        return 'snowflake-default'
    }

    private SnowflakePath toSnowflakePath(Path path) {
        if (!(path instanceof SnowflakePath)) {
            throw new IllegalArgumentException("Path must be a SnowflakePath")
        }
        return (SnowflakePath) path
    }
}

