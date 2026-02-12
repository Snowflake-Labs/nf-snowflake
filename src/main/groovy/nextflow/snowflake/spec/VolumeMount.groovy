package nextflow.snowflake.spec

class VolumeMount {
    String name
    String mountPath

    VolumeMount(String name, String mountPath) {
        this.name = name
        this.mountPath = mountPath
    }
}
