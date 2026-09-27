package compiler

import org.gradle.api.DefaultTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecOperations

import javax.inject.Inject

class NativeImage {
    enum Option {
        STATIC('--static'),
        MUSL('--libc=musl'),
        LINK_BUILD('--link-at-build-time'),
        NO_FALLBACK('--no-fallback')

        final String arg
        Option(String s) { this.arg = s }
    }
}

abstract class NativeImageTask extends DefaultTask {

    @Inject
    abstract ExecOperations getExecOperations()

    @InputFile
    abstract RegularFileProperty getJarFile()

    @Input
    abstract Property<String> getImageName()

    @Input
    abstract Property<String> getExecutable()

    @OutputDirectory
    abstract DirectoryProperty getOutputDir()

    @Input
    abstract ListProperty<Object> getParameters()

    @Input
    abstract Property<Integer> getMinHeap()

    @Input
    abstract Property<Integer> getMaxHeap()

    @Input
    abstract Property<Integer> getMaxNew()

    NativeImageTask() {
        executable.convention('native-image')
        outputDir.convention(project.layout.buildDirectory.dir('native'))
        imageName.convention(project.name)
        minHeap.convention(1)
        maxHeap.convention(32)
        maxNew.convention(32)
        parameters.convention([
            NativeImage.Option.STATIC,
            NativeImage.Option.MUSL,
            NativeImage.Option.LINK_BUILD
        ])
    }

    @OutputFile
    RegularFileProperty getOutputExecutable() {
        outputDir.file(imageName)
    }

    @TaskAction
    void runCommand() {
        def heap = [
            "-R:MinHeapSize=${minHeap.get()}m",
            "-R:MaxHeapSize=${maxHeap.get()}m",
            "-R:MaxNewSize=${maxNew.get()}m"
        ]

        def paramArgs = parameters.get().collect {
            if (it instanceof NativeImage.Option) {
                it.arg
            } else {
                it.toString()
            }
        }

        def jarPath = jarFile.get().asFile.absolutePath
        def targetName = imageName.get()
        def outDir = outputDir.get().asFile
        outDir.mkdirs()

        def command = [executable.get()] + paramArgs + heap + ['-o', targetName, '-jar', jarPath]
        logger.lifecycle "Executing native-image command: '${command.join(' ')}' in ${outDir}"

        execOperations.exec { spec ->
            spec.commandLine command
            spec.workingDir outDir
        }
    }
}

class NativeImagePlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.tasks.register('nativeImage', NativeImageTask) { task ->
            task.group = 'verification'
            task.description = 'Builds a native image from a JAR'
        }
    }
}
