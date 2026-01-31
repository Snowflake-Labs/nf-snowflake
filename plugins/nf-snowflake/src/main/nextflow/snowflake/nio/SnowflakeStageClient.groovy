package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.snowflake.client.jdbc.SnowflakeConnection
import nextflow.snowflake.SnowflakeConnectionPool

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
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeStageClient {

    private final SnowflakeConnectionPool connectionPool

    // Thread-safe date formatter for parsing Snowflake timestamps
    // Format: "Sat, 31 Jan 2026 23:43:33 GMT"
    private static final ThreadLocal<java.text.SimpleDateFormat> DATE_FORMAT =
        ThreadLocal.withInitial(() -> new java.text.SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH))

    SnowflakeStageClient(SnowflakeConnectionPool connectionPool) {
        this.connectionPool = connectionPool
    }

    /**
     * Parse stage path into components
     * 
     * @param stagePath Stage path in format "STAGE_NAME/path/to/file"
     * @return Map with keys: stageName, pathAfterStage, prefix (with trailing /), fileName
     */
    private Map<String, String> parseStagePath(String stagePath) {
        int firstSlash = stagePath.indexOf('/')
        
        if (firstSlash == -1) {
            throw new IllegalArgumentException("Stage path must include a file path: ${stagePath}")
        }
        
        String stageName = stagePath.substring(0, firstSlash)
        String pathAfterStage = stagePath.substring(firstSlash + 1)
        
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
        
        return [
            stageName: stageName,
            pathAfterStage: pathAfterStage,
            prefix: prefix,
            fileName: fileName
        ]
    }

    /**
     * Upload data from an InputStream to a stage location
     * 
     * @param stagePath Stage path in format "STAGE_NAME/path/to/file" (without @ prefix)
     * @param data Input stream containing the data
     * @param size Size of the data in bytes (may be -1 if unknown)
     * @throws IOException if upload fails
     */
    void upload(String stagePath, InputStream data, long size) throws IOException {
        Connection connection = null
        try {
            connection = connectionPool.getConnection()
            
            // Parse stage path into components
            Map<String, String> pathComponents = parseStagePath(stagePath)
            String stageName = pathComponents.stageName
            String prefix = pathComponents.prefix
            String fileName = pathComponents.fileName
            
            // Unwrap to SnowflakeConnection to access uploadStream method
            SnowflakeConnection snowflakeConn = connection.unwrap(SnowflakeConnection.class)
            
            // uploadStream(stageName, prefix, inputStream, destFileName, compress)
            log.debug("Uploading stream to stageName=${stageName}, prefix=${prefix}, file=${fileName}")
            snowflakeConn.uploadStream(stageName, prefix, data, fileName, false)
            
            log.debug("Successfully uploaded to ${stagePath}")
        } catch (SQLException e) {
            log.error("SQL error uploading to ${stagePath}: ${e.message}", e)
            throw new IOException("Failed to upload to ${stagePath}: ${e.message}", e)
        } catch (Exception e) {
            log.error("Unexpected error uploading to ${stagePath}: ${e.message}", e)
            throw new IOException("Failed to upload to ${stagePath}: ${e.message}", e)
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    /**
     * Download data from a stage location to an OutputStream
     * 
     * @param stagePath Stage path in format "STAGE_NAME/path/to/file" (without @ prefix)
     * @param dest Output stream to write the data to
     * @throws IOException if download fails
     */
    void download(String stagePath, OutputStream dest) throws IOException {
        Connection connection = null
        InputStream inputStream = null
        try {
            connection = connectionPool.getConnection()
            
            // Parse stage path into components
            Map<String, String> pathComponents = parseStagePath(stagePath)
            String stageName = pathComponents.stageName
            String pathAfterStage = pathComponents.pathAfterStage
            
            log.debug("Download attempt - stagePath: ${stagePath}, stageName: ${stageName}, pathAfterStage: ${pathAfterStage}")
            
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
            
            log.debug("Successfully downloaded ${totalBytes} bytes from ${stagePath}")
        } catch (SQLException e) {
            log.error("SQLException during download - stagePath: ${stagePath}, message: ${e.message}, sqlState: ${e.getSQLState()}, errorCode: ${e.getErrorCode()}", e)
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                throw new NoSuchFileException(stagePath)
            }
            throw new IOException("Failed to download from ${stagePath}: ${e.message}", e)
        } catch (Exception e) {
            log.error("Unexpected error downloading from ${stagePath}: ${e.class.name}: ${e.message}", e)
            throw new IOException("Failed to download from ${stagePath}: ${e.message}", e)
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
     * @param stagePath Stage path in format "STAGE_NAME/path/" (without @ prefix)
     * @return List of SnowflakeFileAttributes for files in the directory
     * @throws IOException if list operation fails
     */
    List<SnowflakeFileAttributes> list(String stagePath) throws IOException {
        Connection connection = null
        try {
            connection = connectionPool.getConnection()
            
            // Ensure path ends with / for directory listing
            String listPath = stagePath.endsWith('/') ? stagePath : stagePath + '/'
            // Add @ for SQL command
            String listCommand = "LIST @${listPath}"
            log.debug("Executing LIST command for @${stagePath}")
            
            Statement stmt = connection.createStatement()
            try {
                ResultSet rs = stmt.executeQuery(listCommand)
                
                List<SnowflakeFileAttributes> results = []
                while (rs.next()) {
                    String name = rs.getString("name")
                    long size = rs.getLong("size")
                    String md5 = rs.getString("md5")
                    java.sql.Timestamp lastModified = rs.getTimestamp("last_modified")
                    
                    results.add(new SnowflakeFileAttributes(
                        name: name,
                        size: size,
                        md5: md5,
                        lastModified: lastModified ? lastModified.time : 0L
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
     * @param stagePath Stage path in format "STAGE_NAME/path/to/file" (without @ prefix)
     * @throws IOException if delete fails
     */
    void delete(String stagePath) throws IOException {
        Connection connection = null
        try {
            connection = connectionPool.getConnection()
            
            // Add @ for SQL command
            String removeCommand = "REMOVE @${stagePath}"
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
     * @param stagePath Stage path in format "STAGE_NAME/path/to/file" (without @ prefix)
     * @return true if the file or directory exists, false otherwise
     * @throws IOException if the check fails
     */
    boolean exists(String stagePath) throws IOException {
        try {
            SnowflakeFileAttributes attrs = getMetadata(stagePath)
            return attrs != null
        } catch (NoSuchFileException e) {
            // If not found as a file, try as a directory
            try {
                return existsAsDirectory(stagePath)
            } catch (Exception ex) {
                return false
            }
        }
    }

    /**
     * Check if a directory exists by checking if it has any files
     *
     * @param stagePath Stage path in format "STAGE_NAME/path/to/dir" (without @ prefix)
     * @return true if directory exists (has files), false otherwise
     */
    private boolean existsAsDirectory(String stagePath) throws IOException {
        Connection connection = null
        try {
            connection = connectionPool.getConnection()

            // Try to list directory contents with trailing slash
            String listPath = stagePath.endsWith('/') ? stagePath : stagePath + '/'
            String listCommand = "LIST @${listPath}"
            log.debug("Checking directory existence: ${listCommand}")

            Statement stmt = connection.createStatement()
            try {
                ResultSet rs = stmt.executeQuery(listCommand)
                // If we get any results, the directory exists
                boolean hasFiles = rs.next()
                log.debug("Directory ${stagePath} exists: ${hasFiles}")
                return hasFiles
            } finally {
                stmt.close()
            }
        } catch (SQLException e) {
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                return false
            }
            // Other SQL errors - assume directory doesn't exist
            log.debug("Error checking directory ${stagePath}: ${e.message}")
            return false
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    /**
     * Get metadata for a file or directory in a stage
     *
     * @param stagePath Stage path in format "STAGE_NAME/path/to/file" (without @ prefix)
     * @return SnowflakeFileAttributes with file/directory metadata
     * @throws NoSuchFileException if file/directory doesn't exist
     * @throws IOException if the operation fails
     */
    SnowflakeFileAttributes getMetadata(String stagePath) throws IOException {
        Connection connection = null
        try {
            connection = connectionPool.getConnection()

            // First try as a file (no trailing slash)
            String listCommand = "LIST @${stagePath}"
            log.debug("Executing LIST command for @${stagePath}")

            Statement stmt = connection.createStatement()
            try {
                log.debug("Executing query: ${listCommand}")
                ResultSet rs = stmt.executeQuery(listCommand)

                log.debug("ResultSet created, type: ${rs.getClass().name}")
                try {
                    log.debug("ResultSet metadata: columns=${rs.getMetaData().getColumnCount()}")
                } catch (Exception e) {
                    log.debug("Could not get metadata: ${e.message}")
                }

                boolean hasResults = rs.next()
                log.debug("rs.next() returned: ${hasResults}")

                if (hasResults) {
                    String name = null
                    long size = 0
                    String md5 = null
                    java.sql.Timestamp lastModified = null

                    try {
                        name = rs.getString("name")
                        log.debug("Got name: ${name}")
                        size = rs.getLong("size")
                        md5 = rs.getString("md5")

                        // last_modified is returned as a string like "Sat, 31 Jan 2026 23:43:33 GMT"
                        // Try to get as Timestamp first, if that fails, get as String and parse
                        try {
                            lastModified = rs.getTimestamp("last_modified")
                        } catch (Exception e) {
                            String timestampStr = rs.getString("last_modified")
                            if (timestampStr) {
                                // Parse the string format: "Sat, 31 Jan 2026 23:43:33 GMT"
                                java.util.Date date = DATE_FORMAT.get().parse(timestampStr)
                                lastModified = new java.sql.Timestamp(date.time)
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error reading result set columns: ${e.message}", e)
                        throw e
                    }

                    log.debug("LIST returned: name=${name}, requested=${stagePath}")

                    // Check if this is a directory by comparing paths
                    // If the returned path is longer than the requested path,
                    // it means files are inside a subdirectory
                    String normalizedRequested = stagePath.endsWith('/') ? stagePath.substring(0, stagePath.length() - 1) : stagePath
                    String normalizedReturned = name.endsWith('/') ? name.substring(0, name.length() - 1) : name

                    // Case-insensitive comparison (Snowflake returns lowercase paths)
                    if (normalizedReturned.toLowerCase().startsWith(normalizedRequested.toLowerCase() + '/')) {
                        // The returned file is inside the requested path, so requested path is a directory
                        // Return the requested path WITHOUT trailing slash, but set directory=true
                        String dirName = normalizedRequested

                        log.debug("Detected directory: requested=${stagePath}, returned=${name}, dirName=${dirName}")
                        def attrs = new SnowflakeFileAttributes(
                            name: dirName,
                            size: 0L,
                            md5: '',
                            lastModified: lastModified ? lastModified.time : System.currentTimeMillis(),
                            directory: true
                        )
                        log.debug("Returning directory attrs: name=${attrs.name}, isDirectory=${attrs.isDirectory()}")
                        return attrs
                    }

                    // It's a regular file
                    log.debug("Regular file detected: ${name}")
                    return new SnowflakeFileAttributes(
                        name: name,
                        size: size,
                        md5: md5,
                        lastModified: lastModified ? lastModified.time : 0L
                    )
                }

                // No results without trailing slash - try with trailing slash to check if it's a directory
                stmt.close()
                log.debug("No results, trying with trailing slash: LIST @${stagePath}/")

                Statement stmt2 = connection.createStatement()
                try {
                    ResultSet rs2 = stmt2.executeQuery("LIST @${stagePath}/")
                    if (rs2.next()) {
                        // It's a directory - return synthetic directory attributes
                        String firstFileName = rs2.getString("name")
                        log.debug("Directory found with trailing slash, first file: ${firstFileName}")

                        // Return the requested path WITHOUT trailing slash, but set directory=true
                        String dirName = stagePath.endsWith('/') ? stagePath.substring(0, stagePath.length() - 1) : stagePath

                        // Parse timestamp from string format
                        java.sql.Timestamp lastModified = null
                        try {
                            lastModified = rs2.getTimestamp("last_modified")
                        } catch (Exception e) {
                            String timestampStr = rs2.getString("last_modified")
                            if (timestampStr) {
                                java.util.Date date = DATE_FORMAT.get().parse(timestampStr)
                                lastModified = new java.sql.Timestamp(date.time)
                            }
                        }

                        return new SnowflakeFileAttributes(
                            name: dirName,
                            size: 0L,
                            md5: '',
                            lastModified: lastModified ? lastModified.time : System.currentTimeMillis(),
                            directory: true
                        )
                    }
                } finally {
                    stmt2.close()
                }

                log.debug("No results from LIST command for ${stagePath}")
                throw new NoSuchFileException(stagePath)
            } finally {
                stmt.close()
            }
        } catch (SQLException e) {
            if (e.message?.contains("does not exist") || e.message?.contains("not found")) {
                // Not found as a file, try as a directory
                if (existsAsDirectory(stagePath)) {
                    // Return synthetic directory attributes WITHOUT trailing slash, but set directory=true
                    String dirName = stagePath.endsWith('/') ? stagePath.substring(0, stagePath.length() - 1) : stagePath
                    return new SnowflakeFileAttributes(
                        name: dirName,
                        size: 0L,
                        md5: '',
                        lastModified: System.currentTimeMillis(),
                        directory: true
                    )
                }
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

