package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.snowflake.SnowflakeConnectionPool

import java.sql.Connection

/**
 * InputStream implementation for reading from Snowflake stages
 *
 * Wraps the JDBC InputStream and manages the database connection lifecycle.
 * The connection is returned to the pool when the stream is closed.
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeStageInputStream extends InputStream {

    private final InputStream delegate
    private final Connection connection
    private final SnowflakeConnectionPool connectionPool

    SnowflakeStageInputStream(InputStream jdbcStream, Connection connection, SnowflakeConnectionPool connectionPool) {
        this.delegate = jdbcStream
        this.connection = connection
        this.connectionPool = connectionPool
    }

    @Override
    int read() throws IOException {
        return delegate.read()
    }

    @Override
    int read(byte[] b) throws IOException {
        return delegate.read(b)
    }

    @Override
    int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len)
    }

    @Override
    long skip(long n) throws IOException {
        return delegate.skip(n)
    }

    @Override
    int available() throws IOException {
        return delegate.available()
    }

    @Override
    void close() throws IOException {
        try {
            delegate.close()
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection)
            }
        }
    }

    @Override
    synchronized void mark(int readlimit) {
        delegate.mark(readlimit)
    }

    @Override
    synchronized void reset() throws IOException {
        delegate.reset()
    }

    @Override
    boolean markSupported() {
        return delegate.markSupported()
    }
}

