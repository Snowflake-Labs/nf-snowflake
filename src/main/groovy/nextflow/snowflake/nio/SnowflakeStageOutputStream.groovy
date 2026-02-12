package nextflow.snowflake.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.concurrent.Future
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService

/**
 * OutputStream implementation for writing to Snowflake stages
 *
 * Uses piped streams to enable streaming uploads without buffering entire content in memory.
 * Data written to this stream is immediately available to the background upload thread.
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeStageOutputStream extends OutputStream {

    private final SnowflakeStageClient client
    private final SnowflakePath path
    private final PipedOutputStream pipedOutput
    private final PipedInputStream pipedInput
    private final Future<Long> uploadFuture
    private boolean closed = false

    SnowflakeStageOutputStream(SnowflakeStageClient client, SnowflakePath path, ExecutorService executor) {
        this.client = client
        this.path = path

        // Create piped streams with 64KB buffer (default is 1KB which is too small)
        this.pipedInput = new PipedInputStream(64 * 1024)
        this.pipedOutput = new PipedOutputStream(pipedInput)

        // Start async upload immediately
        this.uploadFuture = executor.submit(new java.util.concurrent.Callable<Long>() {
            @Override
            Long call() throws Exception {
                try {
                    log.debug("Background upload started for ${path}")
                    client.upload(path, pipedInput, -1L) // -1 means size unknown
                    long bytesRead = 0
                    // Note: we can't track exact bytes read without wrapping the stream
                    log.debug("Background upload completed for ${path}")
                    return bytesRead
                } catch (Exception e) {
                    log.error("Background upload failed for ${path}", e)
                    throw e
                } finally {
                    try {
                        pipedInput.close()
                    } catch (IOException ignored) {}
                }
            }
        })

        log.debug("Initialized streaming OutputStream for ${path}")
    }

    @Override
    void write(int b) throws IOException {
        checkClosed()
        pipedOutput.write(b)
    }

    @Override
    void write(byte[] b) throws IOException {
        checkClosed()
        pipedOutput.write(b)
    }

    @Override
    void write(byte[] b, int off, int len) throws IOException {
        checkClosed()
        pipedOutput.write(b, off, len)
    }

    @Override
    void flush() throws IOException {
        checkClosed()
        pipedOutput.flush()
    }

    @Override
    void close() throws IOException {
        if (closed) {
            return
        }

        closed = true

        // Close the output pipe to signal EOF to the upload thread
        pipedOutput.close()

        // Wait for upload to complete
        try {
            Long bytesUploaded = uploadFuture.get()
            log.debug("Successfully uploaded to ${path}")
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt()
            throw new IOException("Upload interrupted for ${path}", e)
        } catch (ExecutionException e) {
            throw new IOException("Upload failed for ${path}: ${e.cause?.message}", e.cause)
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed")
        }
    }
}

