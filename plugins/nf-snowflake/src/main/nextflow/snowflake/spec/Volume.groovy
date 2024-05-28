package nextflow.snowflake.spec

class Volume {
    String name
    String source
    Integer uid
    Integer gid

    Volume(String name, String source) {
        this.name = name
        this.source = source
        this.uid = 1234
        this.gid = 5678
    }
}
