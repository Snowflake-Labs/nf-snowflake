package nextflow.snowflake.spec

class Volume {
    String name
    String source
    Integer uid
    Integer gid
    StageConfig stageConfig

    Volume(String name, String source) {
        this.name = name
        this.source = source
        this.uid = 1234
        this.gid = 5678
    }

    Volume(String name, StageConfig stageConfig) {
        this.name = name
        this.stageConfig = stageConfig
        this.source = "stage"
        this.uid = 1234
        this.gid = 5678
    }
}
