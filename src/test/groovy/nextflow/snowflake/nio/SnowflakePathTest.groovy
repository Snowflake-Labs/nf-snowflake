package nextflow.snowflake.nio

import nextflow.snowflake.SnowflakeConnectionPool
import spock.lang.Specification

/**
 * Unit tests for SnowflakePath
 */
class SnowflakePathTest extends Specification {

    SnowflakeFileSystem fileSystem
    
    def setup() {
        def provider = new SnowflakeFileSystemProvider()
        def client = new SnowflakeStageClient(SnowflakeConnectionPool.getInstance())
        fileSystem = new SnowflakeFileSystem(provider, client)
    }

    def 'should parse snowflake URI correctly'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path/to/file.txt')

        when:
        def path = SnowflakePath.parse(fileSystem, uri)

        then:
        path.stageName == 'MY_STAGE'
        path.path == 'path/to/file.txt'
        path.toStageReference() == 'MY_STAGE/path/to/file.txt'
    }
    
    def 'should parse snowflake URI with only stage name'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE')

        when:
        def path = SnowflakePath.parse(fileSystem, uri)

        then:
        path.stageName == 'MY_STAGE'
        path.path == ''
        path.toStageReference() == 'MY_STAGE/'
    }
    
    def 'should handle path with trailing slash'() {
        given:
        def uri = new URI('snowflake://stage/MY_STAGE/path/to/')
        
        when:
        def path = SnowflakePath.parse(fileSystem, uri)
        
        then:
        path.stageName == 'MY_STAGE'
        path.path == 'path/to/'
    }
    
    def 'should reject invalid scheme'() {
        given:
        def uri = new URI('s3://bucket/path')
        
        when:
        SnowflakePath.parse(fileSystem, uri)
        
        then:
        thrown(IllegalArgumentException)
    }
    
    def 'should reject invalid authority'() {
        given:
        def uri = new URI('snowflake://bucket/path')
        
        when:
        SnowflakePath.parse(fileSystem, uri)
        
        then:
        thrown(IllegalArgumentException)
    }
    
    def 'should handle resolve operation'() {
        given:
        def path1 = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to')
        def path2 = new SnowflakePath(fileSystem, '', 'file.txt')
        
        when:
        def resolved = path1.resolve(path2)
        
        then:
        resolved.stageName == 'MY_STAGE'
        resolved.path == 'path/to/file.txt'
    }
    
    def 'should handle getParent operation'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        
        when:
        def parent = path.getParent()
        
        then:
        parent.stageName == 'MY_STAGE'
        parent.path == 'path/to'
    }
    
    def 'should handle getFileName operation'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        
        when:
        def fileName = path.getFileName()
        
        then:
        fileName.path == 'file.txt'
    }
    
    def 'should normalize paths correctly'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/./to/../file.txt')
        
        when:
        def normalized = path.normalize()
        
        then:
        normalized.path == 'path/file.txt'
    }
    
    def 'should handle relativize operation'() {
        given:
        def base = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to')
        def target = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        
        when:
        def relative = base.relativize(target)
        
        then:
        relative.path == 'file.txt'
    }
    
    def 'should check startsWith correctly'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        def prefix = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to')
        
        when:
        def result = path.startsWith(prefix)
        
        then:
        result == true
    }
    
    def 'should check endsWith correctly'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        def suffix = new SnowflakePath(fileSystem, '', 'file.txt')
        
        when:
        def result = path.endsWith(suffix)
        
        then:
        result == true
    }
    
    def 'should convert to URI correctly'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        
        when:
        def uri = path.toUri()
        
        then:
        uri.scheme == 'snowflake'
        uri.authority == 'stage'
        uri.path == '/MY_STAGE/path/to/file.txt'
    }
    
    def 'should compare paths correctly'() {
        given:
        def path1 = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/a.txt')
        def path2 = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/b.txt')
        
        when:
        def result = path1.compareTo(path2)
        
        then:
        result < 0
    }
    
    def 'should handle equals correctly'() {
        given:
        def path1 = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/file.txt')
        def path2 = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/file.txt')
        def path3 = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/other.txt')
        
        expect:
        path1 == path2
        path1 != path3
    }
    
    def 'should iterate over path components'() {
        given:
        def path = new SnowflakePath(fileSystem, 'MY_STAGE', 'path/to/file.txt')
        
        when:
        def components = []
        path.iterator().each { components.add(it.toString()) }
        
        then:
        components == ['MY_STAGE', 'path', 'to', 'file.txt']
    }
}

