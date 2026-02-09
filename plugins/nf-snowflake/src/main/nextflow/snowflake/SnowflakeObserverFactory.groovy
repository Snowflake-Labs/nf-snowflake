package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.plugin.Priority
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceObserverFactoryV2
import nextflow.Session
import nextflow.snowflake.observers.SnowflakeTraceFileObserver

/**
 * Factory for creating Snowflake-aware trace observers
 *
 * Runs with high priority (before DefaultObserverFactory) to intercept
 * trace file creation and use buffered uploads instead of streaming.
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
@Priority(100)  // Load before DefaultObserverFactory (default priority is 0)
class SnowflakeObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        def result = new ArrayList<TraceObserverV2>()

        // Check if trace file observer should be created
        def traceConfig = session.config.trace as Map
        if (traceConfig && traceConfig.enabled) {
            log.info "Creating Snowflake trace file observer with buffered uploads"

            // Create our custom trace observer that uses buffered uploads
            result << new SnowflakeTraceFileObserver(session, traceConfig)

            // Disable default trace observer by modifying session config
            traceConfig.enabled = false
            log.debug "Disabled default TraceFileObserver"
        }

        return result
    }
}
