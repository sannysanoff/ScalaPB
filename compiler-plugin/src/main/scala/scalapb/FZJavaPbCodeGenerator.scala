package scalapb

import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import com.trueaccord.scalapb.Scalapb
import com.trueaccord.scalapb.compiler.{FZProtobufGenerator, ProtobufGenerator}
import protocbridge.{Artifact, ProtocCodeGenerator}


object FZJavaPbCodeGenerator extends ProtocCodeGenerator {
  override def name: String = "fzjava"

  override def run(req: CodeGeneratorRequest): CodeGeneratorResponse = {
    FZProtobufGenerator.handleCodeGeneratorRequest(req)
  }

  override def registerExtensions(registry: ExtensionRegistry): Unit = {
    Scalapb.registerAllExtensions(registry)
  }

  override def suggestedDependencies: Seq[Artifact] = Seq(
    Artifact("com.google.protobuf", "protobuf-java", "3.0.0-beta-2"),
    Artifact("com.trueaccord.scalapb", "scalapb-runtime",
      com.trueaccord.scalapb.compiler.Version.scalapbVersion, crossVersion = true)
  )
}
