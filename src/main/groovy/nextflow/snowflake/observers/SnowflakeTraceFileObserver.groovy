package nextflow.snowflake.observers

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.file.FileHelper
import nextflow.trace.TraceHelper
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceRecord
import nextflow.trace.event.TaskEvent

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * Snowflake-aware TraceFileObserver that buffers trace data in memory
 * and writes at completion to avoid long-running stream timeouts.
 *
 * This observer replaces the default TraceFileObserver when using Snowflake stages,
 * accumulating trace records in memory and writing them all at once when the
 * workflow completes.
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeTraceFileObserver implements TraceObserverV2 {

    static final String DEF_FILE_NAME = 'trace.txt'
    static final String DEF_SEPARATOR = '\t'

    private Path tracePath
    private String separator = DEF_SEPARATOR
    private List<String> fields
    private List<String> formats
    private boolean useRawNumbers
    private boolean overwrite
    private Map<Object, TraceRecord> current = new ConcurrentHashMap<>()

    // Buffer to store trace lines in memory
    private List<String> traceLines = Collections.synchronizedList(new ArrayList<String>())

    SnowflakeTraceFileObserver(Session session, Map config) {
        this.tracePath = config.file ? FileHelper.asPath(config.file as String) : (DEF_FILE_NAME as Path)
        this.overwrite = config.overwrite as Boolean ?: false
        this.separator = config.sep as String ?: DEF_SEPARATOR
        this.useRawNumbers = config.raw as Boolean ?: false

        // Parse fields configuration
        setFieldsAndFormats(config.fields as String)
    }

    protected void setFieldsAndFormats(String fieldsConfig) {
        if (!fieldsConfig) {
            // Default fields if none specified
            this.fields = ['task_id', 'hash', 'native_id', 'name', 'status', 'exit', 'submit', 'duration',
                          'realtime', '%cpu', 'peak_rss', 'peak_vmem', 'rchar', 'wchar']
            this.formats = fields.collect { null }
            return
        }

        // Parse field:format pairs
        def items = fieldsConfig.tokenize(',').collect { it.trim() }
        this.fields = []
        this.formats = []

        for (String item : items) {
            def parts = item.tokenize(':')
            fields.add(parts[0].trim())
            formats.add(parts.size() > 1 ? parts[1].trim() : null)
        }
    }

    @Override
    void onFlowCreate(Session session) {
        log.debug "Flow create -- trace file: $tracePath"

        // Create parent directories
        def parent = tracePath.parent
        if (parent)
            Files.createDirectories(parent)

        // Check if file exists
        if (Files.exists(tracePath) && !overwrite) {
            throw new IllegalArgumentException("Trace file already exists: $tracePath -- enable trace `overwrite` option to replace existing file")
        }

        // Add header line to buffer
        traceLines.add(fields.join(separator))

        log.info "Created trace file observer (buffered mode): $tracePath"
    }

    @Override
    void onTaskSubmit(TaskEvent event) {
        def trace = event.trace
        if (trace)
            current.put(trace.taskId, trace)
    }

    @Override
    void onTaskStart(TaskEvent event) {
        def trace = event.trace
        if (trace)
            current.put(trace.taskId, trace)
    }

    @Override
    void onTaskComplete(TaskEvent event) {
        def trace = event.trace
        if (!trace)
            return

        current.remove(trace.taskId)

        // Add rendered trace line to buffer
        traceLines.add(render(trace))
    }

    @Override
    void onTaskCached(TaskEvent event) {
        def trace = event.trace
        if (!trace)
            return

        // Add rendered trace line to buffer
        traceLines.add(render(trace))
    }

    protected String render(TraceRecord trace) {
        def values = fields.collect { name -> trace.get(name) }
        def result = new ArrayList(values.size())

        for (int i = 0; i < values.size(); i++) {
            def value = values[i]
            def fmt = formats[i]
            result.add(renderValue(value, fmt))
        }

        return result.join(separator)
    }

    protected String renderValue(value, String fmt) {
        if (value == null)
            return ''

        if (fmt && value instanceof Number) {
            try {
                return String.format(fmt, value)
            } catch (Exception e) {
                log.warn "Error formatting value $value with format $fmt: ${e.message}"
            }
        }

        if (useRawNumbers && value instanceof Number)
            return value.toString()

        def str = value.toString()
        // Escape newlines and quotes
        return str.contains('\n') ? "\"${str.replace('"', '""')}\"" : str
    }

    @Override
    void onFlowComplete() {
        log.debug "Flow complete -- writing buffered trace file with ${traceLines.size()} lines"

        try {
            // Write all buffered lines at once using TraceHelper
            def writer = TraceHelper.newFileWriter(tracePath, overwrite, 'UTF-8')
            try {
                traceLines.each { line ->
                    writer.println(line)
                }
                writer.flush()
                log.info "Successfully wrote trace file: $tracePath"
            } finally {
                writer.close()
            }
        } catch (Exception e) {
            log.error("Failed to write trace file: ${e.message}", e)
            throw e
        }
    }
}
