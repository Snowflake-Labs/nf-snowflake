package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * InputStream implementation for reading from Snowflake stages
 * 
 * Downloads file from stage to a temporary file, then streams from that file
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeStageInputStream extends InputStream {

    private final SnowflakeStageClient client
    private final SnowflakePath path
    private InputStream delegate
    private File tempFile

    SnowflakeStageInputStream(SnowflakeStageClient client, SnowflakePath path) {
        this.client = client
        this.path = path
        initialize()
    }

    private void initialize() throws IOException {
        // Create temp file and download stage content
        tempFile = File.createTempFile("snowflake-download-", ".tmp")
        
        try {
            tempFile.withOutputStream { out ->
                client.download(path.toStageReference(), out)
            }
            
            delegate = new FileInputStream(tempFile)
            log.debug("Initialized InputStream for ${path}")
        } catch (IOException e) {
            if (tempFile != null) {
                tempFile.delete()
            }
            throw e
        }
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
            if (delegate != null) {
                delegate.close()
            }
        } finally {
            if (tempFile != null) {
                tempFile.delete()
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

