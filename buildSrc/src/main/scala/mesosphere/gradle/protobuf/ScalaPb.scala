package mesosphere.gradle.protobuf

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.gradle.api.DefaultTask
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecResult

import scala.beans.BeanProperty
import collection.JavaConverters._

/**
  * Gradle task that runs ScalaPb protobuf code generation.
  *
  * @see https://scalapb.github.io/scalapbc.html
  * @see https://github.com/etiennestuder/gradle-jooq-plugin/
  * @see https://github.com/google/protobuf-gradle-plugin
  */
class ScalaPb extends DefaultTask with StrictLogging {

  val runtime =  getProject.getConfigurations.create(ScalaPb.uniqueConfigurationName())
  getProject.getDependencies.add(runtime.getName, "com.thesamet.scalapb:scalapbc_2.12:0.8.4")

  @BeanProperty var protos: FileTree = null

  @BeanProperty var outputDir : File = null

  @TaskAction
  @SuppressWarnings(Array("GroovyUnusedDeclaration"))
  def generate(): Unit = {

    executeScalaPBC()
  }

  def executeScalaPBC(): ExecResult = {
    getProject.javaexec { spec =>
      val classpath = getProject.getBuildscript.getConfigurations.getByName("classpath")
      val protoFilePaths = protos.getFiles.asScala.map(_.getAbsolutePath).toVector
      val protoPath = s"--proto_path=/Users/kjeschkies/Projects/usi/persistence-zookeeper/src/main/protobuf"

      outputDir.mkdir()
      val scalaOut = s"--scala_out=${outputDir.getAbsolutePath}"

      val args = scalaOut +: protoPath +: protoFilePaths

      spec.setMain("scalapb.ScalaPBC")
      spec.setArgs(args.asJava)
      spec.classpath(runtime)
    }
  }
}

object ScalaPb {
  val count = new AtomicInteger()
  def uniqueConfigurationName() = s"scalaPbcRuntime-${count.getAndIncrement()}"
}
