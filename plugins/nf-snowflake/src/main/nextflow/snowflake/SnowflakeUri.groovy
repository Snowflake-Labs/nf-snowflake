package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.net.URLDecoder
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Utility class for parsing and handling Snowflake stage URIs
 *
 * Provides centralized parsing for snowflake:// URIs in the format:
 * snowflake://stage/<stage_name>/path/to/file
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeUri {

    static final String SCHEME = "snowflake://"
    static final String AUTHORITY = "stage/"
    static final String FULL_PREFIX = SCHEME + AUTHORITY

    /**
     * Container for parsed Snowflake URI components
     */
    static class Components {
        final String stageName
        final String path

        Components(String stageName, String path) {
            this.stageName = stageName
            this.path = path
        }

        /** Get the full URI string with proper encoding */
        String toUriString() {
            String encodedStageName = encodePathSegment(stageName)
            if (!path || path.isEmpty()) {
                return "${FULL_PREFIX}${encodedStageName}"
            }

            // Encode each path segment separately to preserve slashes
            String encodedPath = path.split('/').collect { segment ->
                encodePathSegment(segment)
            }.join('/')

            return "${FULL_PREFIX}${encodedStageName}/${encodedPath}"
        }

        /** Encode a single path segment for use in URI */
        private static String encodePathSegment(String segment) {
            if (!segment || segment.isEmpty()) {
                return segment
            }
            return URLEncoder.encode(segment, 'UTF-8').replace('+', '%20')
        }

        /** Get the stage reference format (for Snowflake SQL) */
        String toStageReference() {
            if (!path || path.isEmpty()) {
                return "${stageName}/"
            }
            return "${stageName}/${path}"
        }
    }

    /**
     * Check if a URI string is a Snowflake stage URI
     *
     * @param uriString The URI string to check
     * @return true if the URI is in the format snowflake://stage/...
     */
    static boolean isSnowflakeStageUri(String uriString) {
        return uriString != null && uriString.startsWith(FULL_PREFIX)
    }

    /**
     * Parse a Snowflake URI string into components
     *
     * Format: snowflake://stage/<stage_name>/path/to/file
     *
     * @param uriString The URI string to parse
     * @return Parsed components with stageName and path
     * @throws IllegalArgumentException if the URI is not a valid Snowflake stage URI
     */
    static Components parse(String uriString) {
        if (!isSnowflakeStageUri(uriString)) {
            throw new IllegalArgumentException(
                "Invalid Snowflake URI: ${uriString}. Expected format: ${FULL_PREFIX}<stage_name>/path"
            )
        }

        // Remove the prefix "snowflake://stage/"
        String fullPath = uriString.substring(FULL_PREFIX.length())

        // Remove leading slash if present (shouldn't be there, but handle it)
        if (fullPath.startsWith('/')) {
            fullPath = fullPath.substring(1)
        }

        // Split into stage name and path
        int firstSlash = fullPath.indexOf('/')
        String stageName
        String path

        if (firstSlash == -1) {
            // Just stage name, no path
            stageName = fullPath
            path = ''
        } else {
            stageName = fullPath.substring(0, firstSlash)
            path = fullPath.substring(firstSlash + 1)
        }

        return new Components(stageName, path)
    }

    /**
     * Parse a URI object into components
     *
     * @param uri The URI to parse
     * @return Parsed components with stageName and path
     * @throws IllegalArgumentException if the URI is not valid
     */
    static Components parse(URI uri) {
        // Validate scheme and authority
        if (uri.scheme != 'snowflake' || uri.authority != 'stage') {
            throw new IllegalArgumentException(
                "Invalid Snowflake URI: ${uri}. Expected format: ${FULL_PREFIX}<stage_name>/path"
            )
        }

        // Get the decoded path from URI
        String fullPath = uri.path
        if (!fullPath) {
            throw new IllegalArgumentException("URI path is empty: ${uri}")
        }

        // Remove leading slash
        if (fullPath.startsWith('/')) {
            fullPath = fullPath.substring(1)
        }

        // Split into stage name and path
        int firstSlash = fullPath.indexOf('/')
        String stageName
        String path

        if (firstSlash == -1) {
            // Just stage name, no path
            stageName = URLDecoder.decode(fullPath, 'UTF-8')
            path = ''
        } else {
            stageName = URLDecoder.decode(fullPath.substring(0, firstSlash), 'UTF-8')
            path = URLDecoder.decode(fullPath.substring(firstSlash + 1), 'UTF-8')
        }

        return new Components(stageName, path)
    }

    /**
     * Extract stage name from a Snowflake URI string
     *
     * @param uriString The URI string
     * @return The stage name
     */
    static String extractStageName(String uriString) {
        return parse(uriString).stageName
    }

    /**
     * Translate snowflake:// URI to local mount path
     *
     * Converts snowflake://stage/STAGE_NAME/path/to/file
     * to /mnt/stage/<lowercase_stage_name>/path/to/file
     *
     * @param path Path that may be a snowflake:// URI
     * @return Translated local path or original path
     */
    static Path translateToMount(Path path) {
        if (path == null) {
            return null
        }

        String pathStr = path.toUriString()

        // Check if it's a snowflake:// URI
        if (!isSnowflakeStageUri(pathStr)) {
            return path
        }

        // Use centralized parsing
        Components components = parse(pathStr)
        String stageName = components.stageName.toLowerCase()
        String subPath = components.path

        if (!subPath || subPath.isEmpty()) {
            // Just stage name, no subpath
            return Paths.get("/mnt/stage/${stageName}")
        } else {
            // Translate to /mnt/stage/<lowercase_stage_name>/<subpath>
            return Paths.get("/mnt/stage/${stageName}/${subPath}")
        }
    }
}
