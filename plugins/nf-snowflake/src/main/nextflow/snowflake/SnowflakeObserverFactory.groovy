package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory
import nextflow.Session

@Slf4j
@CompileStatic
class SnowflakeObserverFactory implements TraceObserverFactory {
    @Override
    Collection<TraceObserver> create(Session session) {
        return [new SnowflakeObserver(session)]
    }
}
