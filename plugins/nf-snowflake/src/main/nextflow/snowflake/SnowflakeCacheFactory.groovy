package nextflow.snowflake

import java.nio.file.Path
import java.nio.file.Paths

import groovy.transform.CompileStatic
import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortOperationException
import nextflow.plugin.Priority
import nextflow.cache.CacheDB
import nextflow.cache.CacheFactory

/**
 * Implements the cloud cache factory
 *
 * @see CloudCacheStore
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@CompileStatic
@Priority(-10)
class SnowflakeCacheFactory extends CacheFactory {

    @Override
    protected CacheDB newInstance(UUID uniqueId, String runName, Path home) {
        if( !uniqueId ) throw new AbortOperationException("Missing cache `uuid`")
        if( !runName ) throw new AbortOperationException("Missing cache `runName`")

        // Make sure the cache path exists
        final String basePath = Paths.get(System.getenv("SNOWFLAKE_CACHE_PATH"))
        if ( !basePath) throw new AbortOperationException("Missing SNOWFLAKE_CACHE_PATH")

        final store = new SnowflakeCacheStore(uniqueId, runName, Paths.get(basePath))
        return new CacheDB(store)
    }
}
