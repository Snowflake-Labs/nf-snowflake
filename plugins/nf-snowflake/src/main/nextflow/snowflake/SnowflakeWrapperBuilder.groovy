package nextflow.snowflake

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun
import nextflow.util.Escape

/**
 * Bash wrapper builder for Snowflake executor
 * 
 * Translates snowflake:// URIs to local container mount paths,
 * following the pattern used by FusionScriptLauncher
 *
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeWrapperBuilder extends BashWrapperBuilder {
    
    private Path remoteWorkDir
    
    static SnowflakeWrapperBuilder create(TaskBean bean, SnowflakeExecutor executor) {
        // Store the original remote workDir
        final remoteWorkDir = bean.workDir
        
        // Translate bean work and target dirs to container mount paths
        // This is needed to create the command launcher using container local file paths
        bean.workDir = translateToMount(bean.workDir)
        bean.targetDir = translateToMount(bean.targetDir)
        
        // Remap input files to container mounted paths
        for (Map.Entry<String, Path> entry : new HashMap<>(bean.inputFiles).entrySet()) {
            bean.inputFiles.put(entry.key, translateToMount(entry.value))
        }
        
        // Set header script to change to the task work dir
        bean.headerScript = headerScript(bean)
        
        // Enable use of local scratch dir if not set
        if (bean.scratch == null) {
            bean.scratch = false
        }
        
        // Create the file copy strategy with bin directory support
        final fileCopyStrategy = new SnowflakeFileCopyStrategy(bean, executor)
        
        return new SnowflakeWrapperBuilder(bean, remoteWorkDir, fileCopyStrategy)
    }
    
    private SnowflakeWrapperBuilder(TaskBean bean, Path remoteWorkDir, SnowflakeFileCopyStrategy copyStrategy) {
        super(bean, copyStrategy)
        this.remoteWorkDir = remoteWorkDir
    }
    
    private static String headerScript(TaskBean bean) {
        return "NXF_CHDIR=${Escape.path(bean.workDir)}"
    }
    
    private static Path translateToMount(Path path) {
        if (path == null) {
            return null
        }
        
        String uriStr = path.toUriString()
        if (uriStr.startsWith("snowflake://stage/")) {
            String translated = SnowflakePathHelper.translateSnowflakePathToMount(path)
            return java.nio.file.Paths.get(translated)
        }
        
        return path
    }
    
    /**
     * Returns the original remote workDir (before translation)
     */
    Path getRemoteWorkDir() {
        return remoteWorkDir
    }
    
    // Override file target methods to write to remote snowflake:// paths
    // (not the translated local mount paths)
    
    @Override
    protected Path targetWrapperFile() {
        return remoteWorkDir.resolve(TaskRun.CMD_RUN)
    }

    @Override
    protected Path targetScriptFile() {
        return remoteWorkDir.resolve(TaskRun.CMD_SCRIPT)
    }

    @Override
    protected Path targetInputFile() {
        return remoteWorkDir.resolve(TaskRun.CMD_INFILE)
    }

    @Override
    protected Path targetStageFile() {
        return remoteWorkDir.resolve(TaskRun.CMD_STAGE)
    }
}
