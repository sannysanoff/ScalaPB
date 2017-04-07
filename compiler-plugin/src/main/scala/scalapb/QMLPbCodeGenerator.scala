package scalapb

import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import com.trueaccord.scalapb.Scalapb
import com.trueaccord.scalapb.compiler.QMLProtobufGenerator
import protocbridge.{Artifact, ProtocCodeGenerator}


object QMLPbCodeGenerator extends ProtocCodeGenerator {
  override def name: String = "qml"

  override def run(req: CodeGeneratorRequest): CodeGeneratorResponse = {
    QMLProtobufGenerator.handleCodeGeneratorRequest(req)
  }

  override def registerExtensions(registry: ExtensionRegistry): Unit = {
    Scalapb.registerAllExtensions(registry)
  }

  override def suggestedDependencies: Seq[Artifact] = Seq(
    // Artifact("com.google.protobuf", "protobuf-java", "3.0.0-beta-2"),
  )
}
