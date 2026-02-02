package nextflow.snowflake.nio

import groovy.transform.CompileStatic

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

/**
 * Implements BasicFileAttributes for Snowflake stage files
 * 
 * Provides metadata about files stored in Snowflake stages
 *
 * @author Hongye Yu
 */
@CompileStatic
class SnowflakeFileAttributes implements BasicFileAttributes {

    String name
    long size
    String md5
    long lastModified
    boolean directory = false

    @Override
    FileTime lastModifiedTime() {
        return FileTime.fromMillis(lastModified)
    }

    @Override
    FileTime lastAccessTime() {
        // Snowflake doesn't track access time
        return lastModifiedTime()
    }

    @Override
    FileTime creationTime() {
        // Snowflake doesn't track creation time separately
        return lastModifiedTime()
    }

    @Override
    boolean isRegularFile() {
        return !isDirectory()
    }

    @Override
    boolean isDirectory() {
        // Check the directory flag first, then fall back to checking trailing slash for backwards compatibility
        return directory || (name != null && name.endsWith('/'))
    }

    @Override
    boolean isSymbolicLink() {
        // Snowflake stages don't support symbolic links
        return false
    }

    @Override
    boolean isOther() {
        return false
    }

    @Override
    long size() {
        return size
    }

    @Override
    Object fileKey() {
        // Use full path as file key to ensure uniqueness
        // MD5 is empty for directories, so we can't use it alone
        return name ?: md5
    }

    @Override
    String toString() {
        return "SnowflakeFileAttributes(name=${name}, size=${size}, lastModified=${lastModified})"
    }
}

