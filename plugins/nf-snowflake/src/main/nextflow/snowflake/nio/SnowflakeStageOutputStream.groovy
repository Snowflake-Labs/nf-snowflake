package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * OutputStream implementation for writing to Snowflake stages
 * 
 * Buffers data to a temporary file, then uploads to stage on close()
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeStageOutputStream extends OutputStream {

    private final SnowflakeStageClient client
    private final SnowflakePath path
    private OutputStream delegate
    private File tempFile
    private boolean closed = false

    SnowflakeStageOutputStream(SnowflakeStageClient client, SnowflakePath path) {
        this.client = client
        this.path = path
        initialize()
    }

    private void initialize() throws IOException {
        // Create temp file for buffering
        tempFile = File.createTempFile("snowflake-upload-", ".tmp")
        delegate = new FileOutputStream(tempFile)
        log.debug("Initialized OutputStream for ${path}")
    }

    @Override
    void write(int b) throws IOException {
        checkClosed()
        delegate.write(b)
    }

    @Override
    void write(byte[] b) throws IOException {
        checkClosed()
        delegate.write(b)
    }

    @Override
    void write(byte[] b, int off, int len) throws IOException {
        checkClosed()
        delegate.write(b, off, len)
    }

    @Override
    void flush() throws IOException {
        checkClosed()
        delegate.flush()
    }

    @Override
    void close() throws IOException {
        if (closed) {
            return
        }
        
        closed = true
        
        try {
            // Flush and close the delegate stream
            delegate.flush()
            delegate.close()
            
            // Upload the temp file to the stage
            tempFile.withInputStream { input ->
                client.upload(path, input, tempFile.length())
            }
            
            log.debug("Successfully uploaded to ${path}")
        } finally {
            // Clean up temp file
            if (tempFile != null) {
                tempFile.delete()
            }
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed")
        }
    }
}

