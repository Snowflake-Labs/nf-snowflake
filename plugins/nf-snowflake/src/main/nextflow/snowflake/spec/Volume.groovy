package nextflow.snowflake.spec

class Volume {
    String name
    String source
    StageConfig stageConfig

    Volume(String name, String source) {
        this.name = name
        this.source = source
    }

    Volume(String name, StageConfig stageConfig) {
        this.name = name
        this.stageConfig = stageConfig
        this.source = "stage"
    }
}
