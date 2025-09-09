package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserverV2

/**
 * Implements an observer that allows implementing custom
 */
@Slf4j
@CompileStatic
class SnowflakeObserver implements TraceObserverV2 {

    private final Boolean enableExecutionLog;

    SnowflakeObserver(Boolean enableExecutionLog) {
        this.enableExecutionLog = enableExecutionLog
    }

    @Override
    void onFlowCreate(Session session) {
        println "Pipeline is starting! 🚀"
    }

    @Override
    void onFlowComplete() {
        println "Pipeline complete! 👋"
    }
}
