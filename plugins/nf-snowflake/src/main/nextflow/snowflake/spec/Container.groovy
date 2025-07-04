package nextflow.snowflake.spec

class Container {
    String name

    String image

    List<String> command

    List<VolumeMount> volumeMounts

    Resources resources
}
