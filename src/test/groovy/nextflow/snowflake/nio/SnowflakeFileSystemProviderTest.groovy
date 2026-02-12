package nextflow.snowflake.nio

import spock.lang.Specification

/**
 * Unit tests for SnowflakeFileSystemProvider
 */
class SnowflakeFileSystemProviderTest extends Specification {

    SnowflakeFileSystemProvider provider
    
    def setup() {
        provider = new SnowflakeFileSystemProvider()
    }

    def 'should return correct scheme'() {
        when:
        def scheme = provider.getScheme()
        
        then:
        scheme == 'snowflake'
    }
    
    def 'should create filesystem from URI'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path')
        
        when:
        def fileSystem = provider.getFileSystem(uri)
        
        then:
        fileSystem != null
        fileSystem instanceof SnowflakeFileSystem
    }
    
    def 'should return same filesystem for same URI'() {
        given:
        def uri1 = new URI('snowflake://stage/MY_STAGE/path1')
        def uri2 = new URI('snowflake://stage/MY_STAGE/path2')
        
        when:
        def fs1 = provider.getFileSystem(uri1)
        def fs2 = provider.getFileSystem(uri2)
        
        then:
        fs1.is(fs2)
    }
    
    def 'should parse path from URI'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path/to/file.txt')
        
        when:
        def path = provider.getPath(uri)
        
        then:
        path instanceof SnowflakePath
        path.stageName == 'MY_STAGE'
        path.path == 'path/to/file.txt'
    }
    
    def 'should reject non-snowflake URI'() {
        given:
        def uri = new URI('s3://bucket/path')
        
        when:
        provider.getFileSystem(uri)
        
        then:
        thrown(IllegalArgumentException)
    }
    
    def 'should reject invalid authority'() {
        given:
        def uri = new URI('snowflake://invalid/path')
        
        when:
        provider.getFileSystem(uri)
        
        then:
        thrown(IllegalArgumentException)
    }
    
    def 'should handle isSameFile correctly'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path/file.txt')
        def path1 = provider.getPath(uri)
        def path2 = provider.getPath(uri)
        
        when:
        def result = provider.isSameFile(path1, path2)
        
        then:
        result == true
    }
    
    def 'should report files as not hidden'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path/file.txt')
        def path = provider.getPath(uri)
        
        when:
        def result = provider.isHidden(path)
        
        then:
        result == false
    }
    
    def 'should throw on unsupported operations'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path/file.txt')
        def path = provider.getPath(uri)
        
        when:
        provider.getFileStore(path)
        
        then:
        thrown(UnsupportedOperationException)
        
        when:
        provider.setAttribute(path, 'test', 'value')
        
        then:
        thrown(UnsupportedOperationException)
    }
}

