package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.nio.file.Path

/**
 * Utility class for translating Snowflake stage paths to local mount paths
 * 
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakePathHelper {
    
    /**
     * Translate snowflake:// URI to local mount path
     * 
     * Converts snowflake://stage/STAGE_NAME/path/to/file
     * to /mnt/stage/<lowercase_stage_name>/path/to/file
     * 
     * @param path Path that may be a snowflake:// URI
     * @return Translated local path or original path
     */
    static String translateSnowflakePathToMount(Path path) {
        String pathStr = path.toUriString()
        
        // Check if it's a snowflake:// URI
        if (!pathStr.startsWith("snowflake://stage/")) {
            return pathStr
        }
        
        try {
            // Parse: snowflake://stage/STAGE_NAME/path/to/file
            URI uri = new URI(pathStr)
            String fullPath = uri.path
            if (fullPath.startsWith('/')) {
                fullPath = fullPath.substring(1)
            }
            
            // Split into stage name and remaining path
            int firstSlash = fullPath.indexOf('/')
            if (firstSlash == -1) {
                // Just stage name, no subpath
                String stageName = fullPath
                return "/mnt/stage/${stageName.toLowerCase()}"
            } else {
                String stageName = fullPath.substring(0, firstSlash)
                String subPath = fullPath.substring(firstSlash + 1)
                
                // Translate to /mnt/stage/<lowercase_stage_name>/<subpath>
                return "/mnt/stage/${stageName.toLowerCase()}/${subPath}"
            }
        } catch (Exception e) {
            log.warn("Failed to translate snowflake path: ${pathStr}", e)
            return pathStr
        }
    }
}

