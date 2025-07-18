package nextflow.snowflake.spec

class StageConfig {
    String name
    Boolean enableSymlink

    StageConfig(String name, Boolean enableSymlink) {
        this.name = name
        this.enableSymlink = enableSymlink
    }
}
