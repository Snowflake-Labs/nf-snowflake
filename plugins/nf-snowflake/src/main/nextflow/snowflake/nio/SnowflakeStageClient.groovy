package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.snowflake.client.jdbc.SnowflakeConnection
import nextflow.snowflake.SnowflakeConnectionPool
import nextflow.snowflake.SnowflakeUri

import java.nio.file.NoSuchFileException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.SQLException

/**
 * Client for Snowflake stage operations using JDBC
 * 
 * Wraps JDBC operations with connection pooling and provides
 * high-level methods for stage file operations
 *
 * @author Haowei Yu
 */
@Slf4j
@CompileStatic
class SnowflakeStageClient {

    private final SnowflakeConnectionPool connectionPool

    // Use constant timestamp since Nextflow uses content-based caching (MD5), not timestamps
    // This avoids SimpleDateFormat parsing and serialization issues
    private static final long CONSTANT_TIMESTAMP = 0L

    SnowflakeStageClient(SnowflakeConnectionPool connectionPool) {
        this.connectionPool = connectionPool
    }

    /**
     * Quote a stage reference for use in SQL commands
     * Snowflake requires single quotes around stage paths with spaces or special characters
     */
    private static String quoteStagePath(String stagePath) {
        // Escape any single quotes in the path by doubling them
        String escaped = stagePath.replace("'", "''")
        return "'@${escaped}'"
    }

    /**
     * Upload data from an InputStream to a stage location
     *
     * @param path SnowflakePath pointing to the target location
     * @param data Input stream containing the data
     * @param size Size of the data in bytes (may be -1 if unknown)
     * @throws IOException if upload fails
     */
    void upload(SnowflakePath path, InputStream data, long size) throws IOException {
        Connection connection = null
        try {
            connection = connectionPool.getConnection()

            // Extract components directly from SnowflakePath
            String stageName = path.getStageName()
            String pathAfterStage = path.getPath()

            if (!pathAfterStage) {
                throw new IllegalArgumentException("Path must include a file path: ${path}")
            }

            // Compute prefix (directory path) and fileName
            int lastSlash = pathAfterStage.lastIndexOf('/')
            String prefix
            String fileName

            if (lastSlash == -1) {
                // No subdirectory, just filename
                prefix = ""
                fileName = pathAfterStage
            } else {
                prefix = pathAfterStage.substring(0, lastSlash + 1) // Include trailing /
                fileName = pathAfterStage.substring(lastSlash + 1)
            }

            // Unwrap to SnowflakeConnection to access uploadStream method
            SnowflakeConnection snowflakeConn = connection.unwrap(SnowflakeConnection.class)

            // uploadStream(stageName, prefix, inputStream, destFileName, compress)
            log.debug("Uploading stream to stageName=${stageName}, prefix=${prefix}, file=${fileName}")
            snowflakeConn.uploadStream(stageName, prefix, data, fileName, false)

            log.debug("Successfully uploaded to ${path}")
        } catch (SQLException e) {
            log.error("SQL error uploading to ${path}: ${e.message}", e)
            throw new IOException("Failed to upload to ${path}: ${e.message}", e)
        } catch (Exception e) {
            log.error("Unexpected error uploading to ${path}: ${e.message}", e)
            throw new IOException("Failed to upload to ${path}: ${e.message}", e)
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    /**
     * Get an InputStream for reading from a stage location
     *
     * IMPORTANT: The returned InputStream holds a database connection from the pool.
     * The caller MUST close the stream to return the connection to the pool.
     *
     * @param path SnowflakePath pointing to the source location
     * @return SnowflakeStageInputStream that wraps the JDBC stream and manages connection lifecycle
     * @throws IOException if opening stream fails
     */
    SnowflakeStageInputStream openStream(SnowflakePath path) throws IOException {
        Connection connection = null
        InputStream inputStream = null
        try {
            connection = connectionPool.getConnection()

            // Extract components directly from SnowflakePath
            String stageName = path.getStageName()
            String pathAfterStage = path.getPath()

            log.debug("Opening stream - path: ${path}, stageName: ${stageName}, pathAfterStage: ${pathAfterStage}")

            // Unwrap to SnowflakeConnection to access downloadStream method
            SnowflakeConnection snowflakeConn = connection.unwrap(SnowflakeConnection.class)

            log.debug("Calling downloadStream(${stageName}, ${pathAfterStage}, false)")
            inputStream = snowflakeConn.downloadStream(stageName, pathAfterStage, false)

            // Return SnowflakeStageInputStream that manages connection lifecycle
            return new SnowflakeStageInputStream(inputStream, connection, connectionPool)
        } catch (SQLException e) {
            // Clean up on error
            if (inputStream != null) {
                try { inputStream.close() } catch (IOException ignored) {}
            }
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }

            log.error("SQLException opening stream - path: ${path}, message: ${e.message}", e)
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                throw new NoSuchFileException(path.toString())
            }
            throw new IOException("Failed to open stream from ${path}: ${e.message}", e)
        } catch (Exception e) {
            // Clean up on error
            if (inputStream != null) {
                try { inputStream.close() } catch (IOException ignored) {}
            }
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }

            log.error("Unexpected error opening stream from ${path}: ${e.class.name}: ${e.message}", e)
            throw new IOException("Failed to open stream from ${path}: ${e.message}", e)
        }
    }

    /**
     * Download data from a stage location to an OutputStream
     *
     * @param path SnowflakePath pointing to the source location
     * @param dest Output stream to write the data to
     * @throws IOException if download fails
     */
    void download(SnowflakePath path, OutputStream dest) throws IOException {
        Connection connection = null
        InputStream inputStream = null
        try {
            connection = connectionPool.getConnection()

            // Extract components directly from SnowflakePath
            String stageName = path.getStageName()
            String pathAfterStage = path.getPath()

            log.debug("Download attempt - path: ${path}, stageName: ${stageName}, pathAfterStage: ${pathAfterStage}")

            // Unwrap to SnowflakeConnection to access downloadStream method
            SnowflakeConnection snowflakeConn = connection.unwrap(SnowflakeConnection.class)

            // downloadStream(stageName, srcFileName, decompress)
            // Set decompress=false to disable auto-decompression (files aren't gzipped)
            log.debug("Calling downloadStream(${stageName}, ${pathAfterStage}, false)")
            inputStream = snowflakeConn.downloadStream(stageName, pathAfterStage, false)

            // Stream data to destination
            byte[] buffer = new byte[8192]
            int read
            int totalBytes = 0
            while ((read = inputStream.read(buffer)) != -1) {
                dest.write(buffer, 0, read)
                totalBytes += read
            }

            log.debug("Successfully downloaded ${totalBytes} bytes from ${path}")
        } catch (SQLException e) {
            log.error("SQLException during download - path: ${path}, message: ${e.message}, sqlState: ${e.getSQLState()}, errorCode: ${e.getErrorCode()}", e)
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                throw new NoSuchFileException(path.toString())
            }
            throw new IOException("Failed to download from ${path}: ${e.message}", e)
        } catch (Exception e) {
            log.error("Unexpected error downloading from ${path}: ${e.class.name}: ${e.message}", e)
            throw new IOException("Failed to download from ${path}: ${e.message}", e)
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close()
                } catch (IOException e) {
                    log.warn("Error closing input stream: ${e.message}")
                }
            }
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    /**
     * List files in a stage directory
     *
     * @param path SnowflakePath pointing to the directory
     * @return List of SnowflakeFileAttributes for files in the directory
     * @throws IOException if list operation fails
     */
    List<SnowflakeFileAttributes> list(SnowflakePath path) throws IOException {
        String stagePath = path.toStageReference()
        Connection connection = null
        try {
            connection = connectionPool.getConnection()

            // Ensure path ends with / for directory listing
            String listPath = stagePath.endsWith('/') ? stagePath : stagePath + '/'
            // Quote the stage path for SQL command
            String listCommand = "LIST ${quoteStagePath(listPath)}"
            log.debug("Executing LIST command for @${stagePath}")
            
            Statement stmt = connection.createStatement()
            try {
                ResultSet rs = stmt.executeQuery(listCommand)
                
                List<SnowflakeFileAttributes> results = []
                while (rs.next()) {
                    String name = rs.getString("name")
                    long size = rs.getLong("size")
                    String md5 = rs.getString("md5")
                    // Skip timestamp parsing - use constant value for all files

                    results.add(new SnowflakeFileAttributes(
                        name: name,
                        size: size,
                        md5: md5,
                        lastModified: CONSTANT_TIMESTAMP
                    ))
                }
                
                log.debug("Listed ${results.size()} files in ${stagePath}")
                return results
            } finally {
                stmt.close()
            }
        } catch (SQLException e) {
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                return Collections.emptyList()
            }
            throw new IOException("Failed to list ${stagePath}: ${e.message}", e)
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    /**
     * Delete a file from a stage
     *
     * @param path SnowflakePath pointing to the file to delete
     * @throws IOException if delete fails
     */
    void delete(SnowflakePath path) throws IOException {
        String stagePath = path.toStageReference()
        Connection connection = null
        try {
            connection = connectionPool.getConnection()

            // Quote the stage path for SQL command
            String removeCommand = "REMOVE ${quoteStagePath(stagePath)}"
            log.debug("Executing REMOVE command for @${stagePath}")
            
            Statement stmt = connection.createStatement()
            try {
                stmt.execute(removeCommand)
                log.debug("Successfully deleted @${stagePath}")
            } finally {
                stmt.close()
            }
        } catch (SQLException e) {
            throw new IOException("Failed to delete ${stagePath}: ${e.message}", e)
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    /**
     * Check if a file or directory exists in a stage
     *
     * @param path SnowflakePath pointing to the file or directory
     * @return true if the file or directory exists, false otherwise
     */
    boolean exists(SnowflakePath path) {
        try {
            getMetadata(path)
            return true
        } catch (NoSuchFileException e) {
            return false
        } catch (IOException e) {
            log.debug("Error checking existence of ${path}: ${e.message}")
            return false
        }
    }

    /**
     * Helper to create directory attributes
     */
    private static SnowflakeFileAttributes createDirectoryAttributes(String dirName) {
        // Normalize: remove trailing slash if present
        String normalizedName = dirName.endsWith('/') ? dirName.substring(0, dirName.length() - 1) : dirName
        return new SnowflakeFileAttributes(
            name: normalizedName,
            size: 0L,
            md5: '',
            lastModified: CONSTANT_TIMESTAMP,
            directory: true
        )
    }

    /**
     * Get metadata for a file or directory in a stage
     *
     * @param path SnowflakePath pointing to the file or directory
     * @return SnowflakeFileAttributes with file/directory metadata
     * @throws NoSuchFileException if file/directory doesn't exist
     * @throws IOException if the operation fails
     */
    SnowflakeFileAttributes getMetadata(SnowflakePath path) throws IOException {
        String stagePath = path.toStageReference()
        Connection connection = null
        try {
            connection = connectionPool.getConnection()
            Statement stmt = connection.createStatement()
            try {
                // Try LIST without trailing slash first
                String listCommand = "LIST ${quoteStagePath(stagePath)}"
                log.debug("Executing LIST command: ${listCommand}")
                ResultSet rs = stmt.executeQuery(listCommand)

                if (rs.next()) {
                    String name = rs.getString("name")
                    long size = rs.getLong("size")
                    String md5 = rs.getString("md5")

                    log.debug("LIST returned: name=${name}, requested=${stagePath}")

                    // Check if returned path indicates this is a directory
                    // (returned path has more path components than requested)
                    String normalizedRequested = stagePath.endsWith('/') ?
                        stagePath.substring(0, stagePath.length() - 1) : stagePath
                    String normalizedReturned = name.endsWith('/') ?
                        name.substring(0, name.length() - 1) : name

                    // Case-insensitive comparison (Snowflake returns lowercase)
                    if (normalizedReturned.toLowerCase().startsWith(normalizedRequested.toLowerCase() + '/')) {
                        log.debug("Detected directory: returned file is inside requested path")
                        return createDirectoryAttributes(normalizedRequested)
                    }

                    // It's a regular file
                    log.debug("Regular file detected")
                    return new SnowflakeFileAttributes(
                        name: name,
                        size: size,
                        md5: md5,
                        lastModified: CONSTANT_TIMESTAMP
                    )
                }

                // No results without trailing slash - try with trailing slash for directory
                rs.close()
                log.debug("No results, trying with trailing slash")

                ResultSet rs2 = stmt.executeQuery("LIST ${quoteStagePath(stagePath + '/')}")
                if (rs2.next()) {
                    log.debug("Directory found with trailing slash")
                    return createDirectoryAttributes(stagePath)
                }

                rs2.close()
                log.debug("No results from LIST command for ${stagePath}")
                throw new NoSuchFileException(stagePath)
            } finally {
                stmt.close()
            }
        } catch (SQLException e) {
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                throw new NoSuchFileException(stagePath)
            }
            throw new IOException("Failed to get metadata for ${stagePath}: ${e.message}", e)
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }
}

