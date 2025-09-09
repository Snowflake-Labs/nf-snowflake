package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2
/**
 * Create and register the Snowflake observer instance
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class SnowflakeObserverFactory implements TraceObserverFactoryV2 {
    @Override
    Collection<TraceObserverV2> create(Session session) {
        return [new SnowflakeObserver(true)]
    }
}