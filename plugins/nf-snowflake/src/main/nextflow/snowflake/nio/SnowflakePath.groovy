package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import nextflow.snowflake.SnowflakeUri

import java.net.URLEncoder
import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.nio.file.spi.FileSystemProvider

/**
 * Implements a Path for Snowflake stages
 *
 * URI Format: snowflake://stage/<stage_name>/path/to/file
 * Maps to Snowflake stage reference: @<stage_name>/path/to/file
 *
 * Custom serialization is implemented to avoid serializing the FileSystem reference,
 * which contains non-serializable objects (ConnectionPool, JDBC Connections, etc.)
 *
 * @author Hongye Yu
 */
@CompileStatic
class SnowflakePath implements Path, Serializable {

    private static final long serialVersionUID = 1L

    private final transient SnowflakeFileSystem fileSystem
    private final String stageName
    private final String path
    private final boolean absolute

    /**
     * Creates a SnowflakePath
     * @param fileSystem The filesystem this path belongs to
     * @param stageName The Snowflake stage name (without @ prefix)
     * @param path The path within the stage (without leading /)
     */
    SnowflakePath(SnowflakeFileSystem fileSystem, String stageName, String path) {
        this.fileSystem = fileSystem
        this.stageName = stageName ?: ''
        this.path = path ?: ''
        this.absolute = stageName != null && !stageName.isEmpty()
    }

    /**
     * Parse a URI into stage name and path components
     * Format: snowflake://stage/<stage_name>/path/to/file
     */
    static SnowflakePath parse(SnowflakeFileSystem fileSystem, URI uri) {
        // Use centralized URI parsing
        SnowflakeUri.Components components = SnowflakeUri.parse(uri)
        validatePath(components.path)
        return new SnowflakePath(fileSystem, components.stageName, components.path)
    }

    /**
     * Parse a string path
     */
    static SnowflakePath parse(SnowflakeFileSystem fileSystem, String pathString) {
        // Use centralized URI checking
        if (SnowflakeUri.isSnowflakeStageUri(pathString)) {
            return parse(fileSystem, new URI(pathString))
        }

        // Treat as relative path
        String stageName = ''
        String path = pathString

        // Remove leading slash if present
        if (path.startsWith('/')) {
            path = path.substring(1)
        }

        validatePath(path)
        return new SnowflakePath(fileSystem, stageName, path)
    }

    /**
     * Create SnowflakePath from stage reference format (stage_name/path/to/file)
     * This avoids URI encoding issues with special characters in filenames
     */
    static SnowflakePath fromStageReference(SnowflakeFileSystem fileSystem, String stageRef) {
        if (!stageRef || stageRef.isEmpty()) {
            throw new IllegalArgumentException("Stage reference cannot be null or empty")
        }

        // Split into stage name and path
        int firstSlash = stageRef.indexOf('/')
        String stageName
        String path

        if (firstSlash == -1) {
            // Just stage name, no path
            stageName = stageRef
            path = ''
        } else {
            stageName = stageRef.substring(0, firstSlash)
            path = stageRef.substring(firstSlash + 1)
        }

        validatePath(path)
        return new SnowflakePath(fileSystem, stageName, path)
    }

    /**
     * Validate path for security issues like path traversal
     */
    private static void validatePath(String path) {
        if (!path || path.isEmpty()) {
            return
        }

        // Check for path traversal attempts
        if (path.contains('..')) {
            throw new IllegalArgumentException("Path contains illegal '..' component: ${path}")
        }

        // Check for absolute path attempts that bypass stage
        if (path.startsWith('/')) {
            throw new IllegalArgumentException("Path cannot start with '/': ${path}")
        }
    }

    /**
     * Get the Snowflake stage reference format
     * @return String like "STAGE_NAME/path/to/file" (without @ prefix)
     */
    String toStageReference() {
        if (!stageName) {
            // For relative paths, we can't create a stage reference
            // This might happen during path resolution operations
            throw new IllegalStateException(
                "Cannot convert relative path to stage reference: ${path ?: '(empty)'}"
            )
        }
        
        if (!path || path.isEmpty()) {
            return "${stageName}/"
        }
        
        return "${stageName}/${path}"
    }

    String getStageName() {
        return stageName
    }

    String getPath() {
        return path
    }

    @Override
    FileSystem getFileSystem() {
        return fileSystem
    }

    @Override
    boolean isAbsolute() {
        return absolute
    }

    @Override
    Path getRoot() {
        return absolute ? new SnowflakePath(fileSystem, stageName, '') : null
    }

    @Override
    Path getFileName() {
        if (!path) {
            // For stage-only paths (e.g., "MY_STAGE"), return the stage name as a relative path
            // Never return null to comply with Java NIO Path contract
            return stageName ? new SnowflakePath(fileSystem, '', stageName) : null
        }

        int lastSlash = path.lastIndexOf('/')
        String fileName = lastSlash == -1 ? path : path.substring(lastSlash + 1)
        return new SnowflakePath(fileSystem, '', fileName)
    }

    @Override
    Path getParent() {
        if (!path) {
            // If path is empty, we're at the stage root
            // The parent should be null (can't go higher than stage root)
            return null
        }

        int lastSlash = path.lastIndexOf('/')
        if (lastSlash == -1) {
            // No slashes in path, so parent is the stage root
            return new SnowflakePath(fileSystem, stageName, '')
        }

        String parentPath = path.substring(0, lastSlash)
        return new SnowflakePath(fileSystem, stageName, parentPath)
    }

    @Override
    int getNameCount() {
        if (!path) {
            return stageName ? 1 : 0
        }
        
        return path.split('/').length + (stageName ? 1 : 0)
    }

    @Override
    Path getName(int index) {
        List<String> parts = []
        if (stageName) {
            parts.add(stageName)
        }
        if (path) {
            parts.addAll(path.split('/'))
        }
        
        if (index < 0 || index >= parts.size()) {
            throw new IllegalArgumentException("Invalid index: ${index}")
        }
        
        return new SnowflakePath(fileSystem, '', parts[index])
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        List<String> parts = []
        if (stageName) {
            parts.add(stageName)
        }
        if (path) {
            parts.addAll(path.split('/'))
        }
        
        if (beginIndex < 0 || endIndex > parts.size() || beginIndex >= endIndex) {
            throw new IllegalArgumentException("Invalid subpath indices")
        }
        
        List<String> subParts = parts.subList(beginIndex, endIndex)
        return new SnowflakePath(fileSystem, '', subParts.join('/'))
    }

    @Override
    boolean startsWith(Path other) {
        if (!(other instanceof SnowflakePath)) {
            return false
        }
        
        SnowflakePath otherPath = (SnowflakePath) other
        if (stageName != otherPath.stageName) {
            return false
        }
        
        return path.startsWith(otherPath.path)
    }

    @Override
    boolean startsWith(String other) {
        return startsWith(parse(fileSystem, other))
    }

    @Override
    boolean endsWith(Path other) {
        if (!(other instanceof SnowflakePath)) {
            return false
        }
        
        SnowflakePath otherPath = (SnowflakePath) other
        return path.endsWith(otherPath.path)
    }

    @Override
    boolean endsWith(String other) {
        return endsWith(parse(fileSystem, other))
    }

    @Override
    Path normalize() {
        if (!path) {
            return this
        }
        
        // Resolve . and .. references
        List<String> parts = path.split('/') as List<String>
        List<String> normalized = []
        
        for (String part : parts) {
            if (part == '.' || part.isEmpty()) {
                continue
            } else if (part == '..') {
                if (!normalized.isEmpty()) {
                    normalized.remove(normalized.size() - 1)
                }
            } else {
                normalized.add(part)
            }
        }
        
        return new SnowflakePath(fileSystem, stageName, normalized.join('/'))
    }

    @Override
    Path resolve(Path other) {
        if (other == null) {
            return this
        }
        
        // If it's not a SnowflakePath, convert it to string and resolve
        if (!(other instanceof SnowflakePath)) {
            return resolve(other.toString())
        }
        
        SnowflakePath otherPath = (SnowflakePath) other
        
        if (otherPath.isAbsolute()) {
            return otherPath
        }
        
        if (!otherPath.path) {
            return this
        }
        
        String newPath = path ? "${path}/${otherPath.path}" : otherPath.path
        return new SnowflakePath(fileSystem, stageName, newPath)
    }

    @Override
    Path resolve(String other) {
        return resolve(parse(fileSystem, other))
    }

    @Override
    Path resolveSibling(Path other) {
        Path parent = getParent()
        return parent ? parent.resolve(other) : other
    }

    @Override
    Path resolveSibling(String other) {
        return resolveSibling(parse(fileSystem, other))
    }

    @Override
    Path relativize(Path other) {
        if (!(other instanceof SnowflakePath)) {
            throw new IllegalArgumentException("Cannot relativize non-Snowflake path")
        }

        SnowflakePath otherPath = (SnowflakePath) other

        // Stage names are case-insensitive in Snowflake
        // Both paths must have the same stage name (or both be relative)
        boolean thisHasStage = stageName && !stageName.isEmpty()
        boolean otherHasStage = otherPath.stageName && !otherPath.stageName.isEmpty()

        if (thisHasStage && otherHasStage && !stageName.equalsIgnoreCase(otherPath.stageName)) {
            throw new IllegalArgumentException("Cannot relativize paths with different stage names: this=${stageName}, other=${otherPath.stageName}")
        }

        if (thisHasStage != otherHasStage) {
            throw new IllegalArgumentException("Cannot relativize absolute path against relative path or vice versa")
        }

        // If this path is empty, return the other path as relative
        if (!path || path.isEmpty()) {
            return new SnowflakePath(fileSystem, '', otherPath.path ?: '')
        }

        // If paths are equal, return empty relative path
        if (path == otherPath.path) {
            return new SnowflakePath(fileSystem, '', '')
        }

        // Check if other path starts with this path
        if (otherPath.path.startsWith(path + '/')) {
            String relativePath = otherPath.path.substring(path.length() + 1)
            return new SnowflakePath(fileSystem, '', relativePath)
        }

        // Check if other path starts with this path (without trailing slash check)
        // This handles the case where this.path = "foo" and otherPath.path = "foo"
        if (otherPath.path.startsWith(path)) {
            String suffix = otherPath.path.substring(path.length())
            if (suffix.isEmpty()) {
                return new SnowflakePath(fileSystem, '', '')
            } else if (suffix.startsWith('/')) {
                String result = suffix.substring(1)
                return new SnowflakePath(fileSystem, '', result)
            }
        }

        throw new IllegalArgumentException("Other path does not start with this path: this='${path}', other='${otherPath.path}'")
    }

    @Override
    URI toUri() {
        if (!stageName) {
            // For relative paths, return a file:// URI representation
            // This allows Nextflow to handle them as local paths
            return new File(path ?: '.').toURI()
        }

        // Encode path components to handle spaces and special characters
        // We need to encode each path segment separately to preserve slashes
        String encodedStageName = encodePathSegment(stageName)
        String encodedPath = ''

        if (path) {
            // Split path into segments, encode each, then rejoin with /
            encodedPath = path.split('/').collect { segment ->
                encodePathSegment(segment)
            }.join('/')
        }

        String uriString = encodedPath ?
            "snowflake://stage/${encodedStageName}/${encodedPath}" :
            "snowflake://stage/${encodedStageName}"

        return URI.create(uriString)
    }

    /**
     * Encode a single path segment (between slashes) for use in URI
     */
    private static String encodePathSegment(String segment) {
        if (!segment || segment.isEmpty()) {
            return segment
        }
        // URLEncoder encodes space as +, but URIs need %20
        return URLEncoder.encode(segment, 'UTF-8').replace('+', '%20')
    }

    @Override
    Path toAbsolutePath() {
        if (absolute) {
            return this
        }
        throw new UnsupportedOperationException("Cannot convert relative Snowflake path to absolute: ${this}")
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        // Snowflake stages don't support symbolic links
        return toAbsolutePath()
    }

    @Override
    File toFile() {
        throw new UnsupportedOperationException("Snowflake paths cannot be converted to File")
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException("Watch service not supported for Snowflake paths")
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        throw new UnsupportedOperationException("Watch service not supported for Snowflake paths")
    }

    @Override
    Iterator<Path> iterator() {
        List<String> parts = []
        if (stageName) {
            parts.add(stageName)
        }
        if (path) {
            parts.addAll(path.split('/') as List<String>)
        }
        
        List<Path> pathList = parts.collect { String part -> 
            (Path) new SnowflakePath(fileSystem, '', part) 
        }
        return pathList.iterator()
    }

    @Override
    int compareTo(Path other) {
        if (!(other instanceof SnowflakePath)) {
            throw new ClassCastException("Cannot compare with non-Snowflake path")
        }
        
        SnowflakePath otherPath = (SnowflakePath) other
        
        int stageCompare = (stageName ?: '').compareTo(otherPath.stageName ?: '')
        if (stageCompare != 0) {
            return stageCompare
        }
        
        return (path ?: '').compareTo(otherPath.path ?: '')
    }

    @Override
    String toString() {
        if (!stageName) {
            return path ?: ''
        }
        return path ? "${stageName}/${path}" : stageName
    }

    @Override
    boolean equals(Object obj) {
        if (!(obj instanceof SnowflakePath)) {
            return false
        }
        SnowflakePath other = (SnowflakePath) obj
        return stageName == other.stageName && path == other.path
    }

    @Override
    int hashCode() {
        int result = stageName ? stageName.hashCode() : 0
        result = 31 * result + (path ? path.hashCode() : 0)
        return result
    }

    /**
     * Custom serialization to avoid serializing the FileSystem and its non-serializable references
     * (ConnectionPool, JDBC Connections, SimpleDateFormat, etc.)
     */
    protected Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(stageName, path, absolute)
    }

    /**
     * Serialization proxy class that only stores the essential path components
     */
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L

        private final String stageName
        private final String path
        private final boolean absolute

        SerializedForm(String stageName, String path, boolean absolute) {
            this.stageName = stageName
            this.path = path
            this.absolute = absolute
        }

        /**
         * Reconstruct the SnowflakePath upon deserialization by getting a FileSystem instance
         */
        protected Object readResolve() throws ObjectStreamException {
            try {
                // Use the service loader to find the SnowflakeFileSystemProvider
                FileSystemProvider provider = null
                for (FileSystemProvider p : FileSystemProvider.installedProviders()) {
                    if ('snowflake'.equals(p.getScheme())) {
                        provider = p
                        break
                    }
                }

                if (provider == null) {
                    throw new IllegalStateException("SnowflakeFileSystemProvider not found in installed providers")
                }

                // Get or create a FileSystem using a default URI
                URI defaultUri = new URI('snowflake', 'stage', '/default', null, null)
                FileSystem fs = provider.getFileSystem(defaultUri)

                return new SnowflakePath((SnowflakeFileSystem) fs, stageName, path)
            } catch (Exception e) {
                throw new InvalidObjectException("Failed to deserialize SnowflakePath: " + e.getMessage())
            }
        }
    }
}

