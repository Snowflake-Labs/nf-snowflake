package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.LinkedList
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.Queue
import java.util.Map
import net.snowflake.client.jdbc.SnowflakeConnection
import nextflow.snowflake.config.SnowflakeConfig

@Slf4j
@CompileStatic
class SnowflakeConnectionPool {

    // Connection expiration time: 2 hours in milliseconds
    private static final long CONNECTION_EXPIRY_TIME = 2 * 60 * 60 * 1000L

    // Pool state
    private final Queue<PooledConnection> availableConnections = new LinkedList<>()
    private final Map<String, PooledConnection> allConnections = new HashMap<>()
    private final ReentrantLock poolLock = new ReentrantLock()

    // Configuration
    private SnowflakeConfig config

    // Singleton instance
    private static final SnowflakeConnectionPool INSTANCE = new SnowflakeConnectionPool()

    static {
        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
    }

    private SnowflakeConnectionPool() {
    }

    /**
     * Set the Snowflake configuration for connection creation
     */
    void setConfig(SnowflakeConfig config) {
        this.config = config
    }

    public static SnowflakeConnectionPool getInstance() {
        return INSTANCE
    }

    /**
     * Extract sessionID from a Connection by unwrapping to SnowflakeConnection
     */
    private String getSessionId(Connection connection) {
        try {
            SnowflakeConnection snowflakeConnection = connection.unwrap(SnowflakeConnection.class)
            return snowflakeConnection.getSessionID()
        } catch (SQLException e) {
            log.warn("Failed to get session ID from connection: ${e.message}")
            throw new RuntimeException("Unable to extract session ID", e)
        }
    }

    /**
     * Get a connection from the pool
     */
    Connection getConnection() {
        poolLock.lock()
        try {
            // Try to get an available connection
            PooledConnection pooledConn = getValidConnection()

            if (pooledConn == null) {
                    pooledConn = createNewPooledConnection()
            }

            if (pooledConn != null) {
                pooledConn.lastBorrowTime = System.currentTimeMillis()
                log.debug("Retrieved connection from pool. Total connections: ${allConnections.size()}")
                return pooledConn.connection
            }

            throw new SQLException("Unable to obtain connection from pool")

        } finally {
            poolLock.unlock()
        }
    }

    /**
     * Return a connection to the pool
     */
    void returnConnection(Connection connection) {
        if (connection == null) return

        poolLock.lock()
        try {
            String sessionId = getSessionId(connection)
            PooledConnection pooledConn = allConnections.get(sessionId)
            if (pooledConn != null) {
                // Update the return timestamp
                pooledConn.lastReturnTime = System.currentTimeMillis()

                // Check if connection is still valid (but don't check expiration on return)
                if (isConnectionPhysicallyValid(pooledConn)) {
                    availableConnections.offer(pooledConn)
                    log.debug("Returned connection to pool")
                } else {
                    // Connection is physically invalid, remove it
                    removeConnection(pooledConn)
                    log.debug("Removed invalid connection from pool")
                }
            }
        } finally {
            poolLock.unlock()
        }
    }

    /**
     * Get a valid connection from available connections
     */
    private PooledConnection getValidConnection() {
        PooledConnection pooledConn = availableConnections.poll()
        while (pooledConn != null) {
            if (isConnectionExpired(pooledConn)) {
                // Connection expired, remove it and try next available connection
                log.debug("Connection expired after 2 hours, removing and trying next available connection")
                removeConnection(pooledConn)
                pooledConn = availableConnections.poll()
            } else { // connection not expired
                if (isConnectionPhysicallyValid(pooledConn)) {
                    return pooledConn
                } else {
                    // Remove physically invalid connection
                    removeConnection(pooledConn)
                    pooledConn = availableConnections.poll()
                }
            }
        }
        return null
    }

    /**
     * Create a new pooled connection
     */
    private PooledConnection createNewPooledConnection() {
        try {
            Connection rawConnection = createRawConnection()
            String sessionId = getSessionId(rawConnection)
            PooledConnection pooledConn = new PooledConnection(rawConnection, sessionId)
            allConnections.put(sessionId, pooledConn)
            log.debug("Created new connection. Total connections: ${allConnections.size()}")
            return pooledConn
        } catch (Exception e) {
            log.error("Failed to create new connection: ${e.message}", e)
            return null
        }
    }

    /**
     * Create a raw database connection
     */
    private Connection createRawConnection() {
        final java.nio.file.Path tokenPath = Paths.get("/snowflake/session/token")

        // If token file exists, use token-based authentication
        if (Files.exists(tokenPath)) {
            log.debug("Using token-based authentication from /snowflake/session/token")
            final String token = new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8)

            final Properties properties = new Properties()
            properties.put("account", System.getenv("SNOWFLAKE_ACCOUNT"))
            properties.put("database", System.getenv("SNOWFLAKE_DATABASE"))
            properties.put("schema", System.getenv("SNOWFLAKE_SCHEMA"))
            properties.put("authenticator", "oauth")
            properties.put("token", token)
            properties.put("insecureMode", "true")
            final String wh = System.getenv("SNOWFLAKE_WAREHOUSE")
            if (wh != null) {
                properties.put("warehouse", wh)
            }

            return DriverManager.getConnection(
                    String.format("jdbc:snowflake://%s", System.getenv("SNOWFLAKE_HOST")),
                    properties)
        } else {
            // Fall back to connections.toml via jdbc:snowflake:auto
            // Use connectionName from config if available
            String jdbcUrl = "jdbc:snowflake:auto"
            if (config?.connectionName) {
                jdbcUrl = "jdbc:snowflake:auto?connectionName=${config.connectionName}"
                log.debug("Using connection name from config: ${config.connectionName}")
            } else {
                log.debug("No connection name specified, using jdbc:snowflake:auto (will use default connection)")
            }
            return DriverManager.getConnection(jdbcUrl)
        }
    }

    /**
     * Check if a connection has expired based on when it was returned to the pool
     */
    private boolean isConnectionExpired(PooledConnection pooledConn) {
        if (pooledConn == null) {
            return true
        }

        long currentTime = System.currentTimeMillis()
        long timeInPool = currentTime - pooledConn.lastReturnTime

        if (timeInPool > CONNECTION_EXPIRY_TIME) {
            log.debug("Connection expired after being in pool for ${timeInPool} ms (limit: ${CONNECTION_EXPIRY_TIME} ms)")
            return true
        }

        return false
    }

    /**
     * Check if a connection is physically valid (not closed, responsive)
     */
    private boolean isConnectionPhysicallyValid(PooledConnection pooledConn) {
        if (pooledConn == null || pooledConn.connection == null) {
            return false
        }

        // Check if connection is still alive
        try {
            return !pooledConn.connection.isClosed()
        } catch (SQLException e) {
            log.debug("Connection physical validation failed: ${e.message}")
            return false
        }
    }

    /**
     * Remove a connection from the pool
     */
    private void removeConnection(PooledConnection pooledConn) {
        if (pooledConn == null) return

        try {
            allConnections.remove(pooledConn.sessionId)

            if (!pooledConn.connection.isClosed()) {
                pooledConn.connection.close()
            }
        } catch (SQLException e) {
            log.warn("Error closing connection: ${e.message}")
        }
    }

    /**
     * Inner class to hold connection metadata
     */
    private static class PooledConnection {
        final Connection connection
        final String sessionId
        final long creationTime
        long lastBorrowTime
        long lastReturnTime

        PooledConnection(Connection connection, String sessionId) {
            this.connection = connection
            this.sessionId = sessionId
            this.creationTime = System.currentTimeMillis()
            this.lastBorrowTime = creationTime
            this.lastReturnTime = creationTime  // Initially set to creation time
        }
    }
}
