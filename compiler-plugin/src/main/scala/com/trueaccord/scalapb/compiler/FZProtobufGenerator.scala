package com.trueaccord.scalapb.compiler

import com.google.protobuf.Descriptors._
import com.google.protobuf.{ByteString => GoogleByteString}
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import scala.collection.JavaConversions._

class FZProtobufGenerator(val params: GeneratorParams) extends FZDescriptorPimps {

  def printEnum(e: EnumDescriptor, printer: FunctionalPrinter): FunctionalPrinter = {
    val name = e.nameSymbol
    printer
      .add(s"public class $name implements java.io.Serializable {")
      .indent
      .print(e.getValues) {
        case (v, p) => p.add(
          s"public static final int ${v.getName} = ${v.getNumber};")
      }
      .outdent
      .add("}")
  }

  def printEnumNoClass(e: EnumDescriptor, printer: FunctionalPrinter): FunctionalPrinter = {
    val name = e.nameSymbol
    printer
      .print(e.getValues) {
        case (v, p) => p.add(
          s"public static final int ${v.getName} = ${v.getNumber};")
      }
  }


  def escapeString(raw: String): String = raw.map {
    case u if u >= ' ' && u <= '~' => u.toString
    case '\b' => "\\b"
    case '\f' => "\\f"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case '\\' => "\\\\"
    case '\"' => "\\\""
    case '\'' => "\\\'"
    case c: Char => "\\u%4s".format(c.toInt.toHexString).replace(' ', '0')
  }.mkString("\"", "", "\"")


  def javaToScalaConversion(field: FieldDescriptor) = {
    val baseValueConversion = field.getJavaType match {
      case FieldDescriptor.JavaType.INT => MethodApplication("intValue")

      case FieldDescriptor.JavaType.LONG => MethodApplication("longValue")
      case FieldDescriptor.JavaType.FLOAT => MethodApplication("floatValue")
      case FieldDescriptor.JavaType.DOUBLE => MethodApplication("doubleValue")
      case FieldDescriptor.JavaType.BOOLEAN => MethodApplication("booleanValue")
      case FieldDescriptor.JavaType.BYTE_STRING => Identity
      case FieldDescriptor.JavaType.STRING => Identity
      case FieldDescriptor.JavaType.MESSAGE => FunctionApplication(
        field.getMessageType.scalaTypeName + ".fromJavaProto")
      case FieldDescriptor.JavaType.ENUM => FunctionApplication(
        field.getEnumType.scalaTypeName + ".fromJavaValue")
    }
    baseValueConversion andThen toCustomTypeExpr(field)
  }


  def javaFieldToScala(container: String, field: FieldDescriptor) = {
    val javaHas = container + ".has" + field.upperJavaName
    val javaGetter = if (field.isRepeated)
      container + ".get" + field.upperJavaName + "List"
    else
      container + ".get" + field.upperJavaName

    val valueConversion = javaToScalaConversion(field)

    if (field.supportsPresence)
      s"if ($javaHas) Some(${valueConversion.apply(javaGetter, isCollection = false)}) else None"
    else valueConversion(javaGetter, isCollection = field.isRepeated)
  }


  def toBaseTypeExpr(field: FieldDescriptor) =
    if (field.customSingleScalaTypeName.isDefined) FunctionApplication(field.typeMapper + ".toBase")
    else Identity

  def toBaseFieldType(field: FieldDescriptor) = if (field.isEnum)
    (toBaseTypeExpr(field) andThen MethodApplication("valueDescriptor"))
  else toBaseTypeExpr(field)

  def toBaseType(field: FieldDescriptor)(expr: String) =
    toBaseTypeExpr(field).apply(expr, isCollection = false)

  def toCustomTypeExpr(field: FieldDescriptor) =
    if (field.customSingleScalaTypeName.isEmpty) Identity
    else FunctionApplication(s"${field.typeMapper}.toCustom")

  def toCustomType(field: FieldDescriptor)(expr: String) =
    toCustomTypeExpr(field).apply(expr, isCollection = false)


  def generateWriteOne(field: FieldDescriptor, valueName: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    field.getJavaType match {
      case FieldDescriptor.JavaType.ENUM =>
        printer.add(s"out.writeInt32(${field.getNumber}, $valueName);")
      case _ =>
        printer.add(s"out.write${Types.capitalizedType(field.getType)}(${field.getNumber}, $valueName);")
    }
  }

  def generateReadOne(field: FieldDescriptor, valueName: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    field.getJavaType match {
      case FieldDescriptor.JavaType.ENUM =>
        printer.add(s"out.readInt32();")
      case _ =>
        printer.add(s"out.write${Types.capitalizedType(field.getType)}(${field.getNumber}, $valueName);")
    }
  }

  def defaultValue(field: FieldDescriptor): String = field.getJavaType match {
    case FieldDescriptor.JavaType.ENUM => "0"
    case FieldDescriptor.JavaType.INT => "0"
    case FieldDescriptor.JavaType.LONG => "0"
    case FieldDescriptor.JavaType.FLOAT => "0"
    case FieldDescriptor.JavaType.DOUBLE => "0"
    case FieldDescriptor.JavaType.BOOLEAN => "false"
    case _ => "null"
  }

  def printMessage(topLevel: Boolean)(message: Descriptor,
                                      printer: FunctionalPrinter): FunctionalPrinter = {
    val requiredFields = message.fields.filter(!_.isOptional).map(f => s"${f.javaTypeName} ${f.getName}") mkString ", "

    printer
      .add("")
      .add("/**")
      .add(s"  * ${message.getName}")
      .add("  */")
      .add(s"public ${if (topLevel) "" else "static "}class ${message.nameSymbol} implements Message, java.io.Serializable {")
      .indent

      .print(message.fields) {
        case (field, p) =>
          p.add(s"protected ${field.javaTypeName} ${field.getName}${field.initializer};")
            .when(field.isOptional)(p => p.add(s"protected boolean _has_${field.getName};"))
      }

      .add("")
      .when(requiredFields.nonEmpty)(
        _.add("/**")
          .add(" * Default constructor w/o parameters")
          .add(" */")
          .add(s"public ${message.nameSymbol} () {")
          .add("}")
          .add("")
          .add("/**")
          .add(" * Default constructor with required params")
          .add(" */")
          .add(s"public ${message.nameSymbol} ($requiredFields) {" )
          .indent
          .add("// Members initialization")
          .print(message.fields) {
            case (field, p) => { p
              .when(!field.isOptional)(p => p
                .add(s"this.${field.getName} = ${field.getName};")
              )
            }
          }
          .outdent
          .add("}")
          .add("")
      )

      .print(message.getEnumTypes)(printEnumNoClass)
      .print(message.nestedTypes)(printMessage(topLevel = false))

      .print(message.fields) {
        case (field, p) => {
          p.add(s"public ${field.javaTypeName} get${field.upperJavaName}${if (field.isRepeated) "ArrayList" else ""}() {return ${field.getName};} ")
            .when(field.isRepeated)(p => p
              .add(s"public ${field.singleJavaTypeName} get${field.upperJavaName}(int i) {return ${field.getName}.get(i);} ")
            )
            .when(field.isRepeated || !field.isOptional)(p =>
              p.add(s"public void set${field.upperJavaName}${if (field.isRepeated) "ArrayList" else ""}(${field.javaTypeName} val) {this.${field.getName} = val;} "))
            .when(field.isOptional)(p =>
              p.add(s"public boolean has${field.upperJavaName}() {return _has_${field.getName};} ")
                .add(s"public void clear${field.upperJavaName}() {this.${field.getName} = ${defaultValue(field)}; _has_${field.getName} = false;} ")
                .add(s"public void set${field.upperJavaName}(${field.javaTypeName} val) {this.${field.getName} = val; _has_${field.getName} = true;} "))
            .when(field.isRepeated)(p =>
              p.add(s"public void add${field.upperJavaName}(${field.singleJavaTypeName} item) {this.${field.getName}.add(item);} ")
                .add(s"public int get${field.upperJavaName}Count() {return this.${field.getName}.size();} ")
            )
        }
      }
      .add("")
      .add("public final void serialize(CodedOutputStream out) throws IOException {")
      .indent
      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(p => p
              .add(s"if (_has_${field.getName})")
              .indent
              .call(generateWriteOne(field, field.getName))
              .outdent)
            .when(field.isRepeated)(p => p
              .add(s"for(${field.singleJavaTypeName} q : ${field.getName})")
              .indent
              .call(generateWriteOne(field, "q"))
              .outdent)
            .when(!field.isRepeated && !field.isOptional)(p => p
              .call(generateWriteOne(field, field.getName)))
        }
      }
      .outdent
      .add("}")

      .add("")
      .add("public final void deserialize(CodedInputStream in) throws IOException {")
      .indent
      .add("while (true) {")
      .indent
      .add("int tag = in.readTag();")
      .add("switch(tag) {")
      .indent
      .add("case 0: return;")
      .print(message.fields) {
        case (field, p) => p
          .add(s"case ${(field.getNumber << 3) + Types.wireType(field.getType)}: ")
          .indent
          .when(!field.isRepeated)(p =>
            (field.getJavaType match {
              case FieldDescriptor.JavaType.ENUM =>
                p.add(s"set${field.upperJavaName}(in.readInt32());")
              case FieldDescriptor.JavaType.MESSAGE =>
                p.add(s"set${field.upperJavaName}(new ${field.singleJavaTypeName}());")
                  .add(s"in.readMessage(${field.getName});")
              case _ =>
                p.add(s"set${field.upperJavaName}(in.read${Types.capitalizedType(field.getType)}());")
            })
              .add("break;"))
          .when(field.isRepeated && field.getJavaType == FieldDescriptor.JavaType.MESSAGE)(p => p
            .add(s"{${field.singleJavaTypeName} message = new ${field.singleJavaTypeName}();")
            .add("in.readMessage(message);")
            .add(s"add${field.upperJavaName}(message);}")
            .add("break;")
          )
          .when(field.isRepeated && field.getJavaType != FieldDescriptor.JavaType.MESSAGE)(p => p
            .add(s"add${field.upperJavaName}(in.read${Types.capitalizedType(field.getType)}());")
            .add("break;")
          )
          .outdent
      }
      .add("default: in.skipTag(tag);")
      .outdent
      .add("}")
      .outdent
      .add("}")
      .outdent
      .add("}")

      .addM(
        s"""
           |public static ${message.nameSymbol} fromBytes(byte[] in) throws EncodingException {
           |    ${message.nameSymbol} message = new ${message.nameSymbol}();
           |    ProtoUtil.messageFromBytes(in, message);
           |    return message;
           |}
           |
           |public byte[] toBytes() throws EncodingException {
           |    return ProtoUtil.messageToBytes(this);
           |}
           """.stripMargin)


      .add("@Override")
      .add("public String toString() {")
      .indent
      .add("String res = \"{ " + message.nameSymbol + "\";")
      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(p => p
              .add(s"if (_has_${field.getName})")
              .indent
              .call(generateToString(field, field.getName, field.getName))
              .outdent)
            .when(field.isRepeated)(p => p
              .add(s"for(${field.singleJavaTypeName} q : ${field.getName}) {")
              .indent
              .call(generateToString(field, "q", field.getName + "[]"))
              .outdent
              .add("}"))
            .when(!field.isRepeated && !field.isOptional)(p => p
              .call(generateToString(field, field.getName, field.getName)))
        }
      }
      .add("return res + \"}\";")
      .outdent
      .add("}")

      .add("")
      .add("@Override")
      .add("public boolean equals(Object o) {")
      .indent
      .add("if (this == o) return true;")
      .add("if (!(o instanceof " + message.nameSymbol + ")) return false;")
      .add("")
      .add(message.nameSymbol + " that = (" + message.nameSymbol + ") o;")
      .add("")

      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(p => p
              .add(s"if (_has_${field.getName} != that._has_${field.getName}) return false;")
              .indent
              .call(generateEquals(field, field.getName, "that." + field.getName))
              .outdent
              )
            .when(field.isRepeated)(p => p
              .add(s"for(int i = 0; i < ${field.getName}.size(); ++i) {")
              .indent
              .call(generateEquals(field, s"this.${field.getName}.get(i)", s"that.${field.getName}.get(i)"))
              .outdent
              .add("}")
              )
            .when(!field.isRepeated && !field.isOptional)(p => p
              .call(generateEquals(field, field.getName, "that." + field.getName)))
        }
      }

      .add("return true;")
      .outdent
      .add("}")

      .add("")
      .add("@Override")
      .add("public int hashCode() {")
      .indent
      .add("int result = 0;")

      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(p => p
              .add(s"if (_has_${field.getName}) {")
              .indent
              .call(generateHashcode(field, field.getName))
              .outdent
              .add(s"} else {")
              .indent
              .add(s"result += 1;")
              .outdent
              .add("}")
            )
            .when(field.isRepeated)(p => p
              .add(s"result = 31 * result + (${field.getName} != null ? ${field.getName}.hashCode() : 0);")
            )
            .when(!field.isRepeated && !field.isOptional)(p => p
              .call(generateHashcode(field, field.getName))
            )
        }
      }

      .add("return result;")
      .outdent
      .add("}")

      .outdent
      .add("}")
      .add("")
  }

  def generateHashcode(field: FieldDescriptor, valueName: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    field.getJavaType match {
      case FieldDescriptor.JavaType.INT =>
        printer.add(s"result = 31 * result + ${valueName};")
      case FieldDescriptor.JavaType.LONG =>
        printer.add(s"result = 31 * result + (int)(${valueName});")
      case FieldDescriptor.JavaType.FLOAT =>
        printer.add(s"result = 31 * result + new Float(${valueName}).hashCode();")
      case FieldDescriptor.JavaType.DOUBLE =>
        printer.add(s"result = 31 * result + new Double(${valueName}).hashCode();")
      case FieldDescriptor.JavaType.BOOLEAN =>
        printer.add(s"result = 31 * result + (${valueName} ? 1 : 0);")
      case FieldDescriptor.JavaType.ENUM =>
        printer.add(s"result = 31 * result + ${valueName};")
      case _ => {
        printer.add(s"result = 31 * result + ${valueName}.hashCode();")
      }
    }
  }

  def generateEquals(field: FieldDescriptor, valueName1: String, valueName2: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    field.getJavaType match {
      case FieldDescriptor.JavaType.INT =>
        printer.add(s"if (${valueName1} != ${valueName2}) return false;")
      case FieldDescriptor.JavaType.LONG =>
        printer.add(s"if (${valueName1} != ${valueName2}) return false;")
      case FieldDescriptor.JavaType.BOOLEAN =>
        printer.add(s"if (${valueName1} != ${valueName2}) return false;")
      case FieldDescriptor.JavaType.ENUM =>
        printer.add(s"if (${valueName1} != ${valueName2}) return false;")
      case _ => {
        printer.add(s"if (${valueName1} != null && ${valueName2} != null && !${valueName1}.equals(${valueName2})) return false;",
          s"if (${valueName1} != null && ${valueName2} == null) return false;",
          s"if (${valueName1} == null && ${valueName2} != null) return false;")
      }
    }
  }

  def generateToString(field: FieldDescriptor, valueName: String, strValueName: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    def q = "\""
    def space = s"$q, ${strValueName}=$q"
    field.getJavaType match {
      case FieldDescriptor.JavaType.ENUM =>
        printer.add(s"res += $space + $valueName;")
      case _ =>
        printer.add(s"res += $space + $valueName;")
    }
  }


  val javaImportList = Seq("java.util.ArrayList", "java.io.IOException", "com.ponderingpanda.protobuf.*")

  def javaFileHeader(file: FileDescriptor): FunctionalPrinter = {
    new FunctionalPrinter().addM(
      s"""
         |// Generated by the FZJava Plugin for the Protocol Buffer Compiler.
         |// Do not edit!
         |//
         |// Protofile syntax: ${file.getSyntax.toString}
         |
         |${if (file.scalaPackageName.nonEmpty) "package " + file.scalaPackageName + ";" else ""}
         |
         |""")
      .print(javaImportList) {
        case (i, printer) => printer.add(s"import $i;")
      }
      .add("")
  }

  def generateScalaFilesForFileDescriptor(file: FileDescriptor): Seq[CodeGeneratorResponse.File] = {
    val serviceFiles = if (params.grpc) {
      file.getServices.map {
        service =>
          val p = new GrpcServicePrinter(service, params)
          val code = p.printService(FunctionalPrinter()).result()
          val b = CodeGeneratorResponse.File.newBuilder()
          b.setName(file.scalaPackageName.replace('.', '/') + "/" + service.objectName + ".java")
          b.setContent(code)
          b.build
      }
    } else Nil

    val enumFiles = for {
      enum <- file.getEnumTypes
    } yield {
      val b = CodeGeneratorResponse.File.newBuilder()
      b.setName(file.scalaPackageName.replace('.', '/') + "/" + enum.getName + ".java")
      b.setContent(
        javaFileHeader(file)
          .call(printEnum(enum, _)).result())
      b.build
    }

    val messageFiles = for {
      message <- file.getMessageTypes
    } yield {
      val b = CodeGeneratorResponse.File.newBuilder()
      b.setName(file.scalaPackageName.replace('.', '/') + "/" + message.scalaName + ".java")
      b.setContent(
        javaFileHeader(file)
          .call(printMessage(true)(message, _)).result())
      b.build
    }

    serviceFiles ++ enumFiles ++ messageFiles // :+ fileDescriptorObjectFile
  }
}

object FZProtobufGenerator {
  private def parseParameters(params: String): Either[String, GeneratorParams] = {
    params.split(",").map(_.trim).filter(_.nonEmpty).foldLeft[Either[String, GeneratorParams]](Right(GeneratorParams())) {
      case (Right(params), "java_conversions") => Right(params.copy(javaConversions = true))
      case (Right(params), "flat_package") => Right(params.copy(flatPackage = true))
      case (Right(params), "grpc") => Right(params.copy(grpc = true))
      case (Right(params), p) => Left(s"Unrecognized parameter: '$p'")
      case (x, _) => x
    }
  }

  def handleCodeGeneratorRequest(request: CodeGeneratorRequest): CodeGeneratorResponse = {
    val b = CodeGeneratorResponse.newBuilder
    parseParameters(request.getParameter) match {
      case Right(params) =>
        val generator = new FZProtobufGenerator(params)
        val filesByName: Map[String, FileDescriptor] =
          request.getProtoFileList.foldLeft[Map[String, FileDescriptor]](Map.empty) {
            case (acc, fp) =>
              val deps = fp.getDependencyList.map(acc)
              acc + (fp.getName -> FileDescriptor.buildFrom(fp, deps.toArray))
          }
        request.getFileToGenerateList.foreach {
          name =>
            val file = filesByName(name)
            val responseFiles = generator.generateScalaFilesForFileDescriptor(file)
            b.addAllFile(responseFiles)
        }
      case Left(error) =>
        b.setError(error)
    }
    b.build
  }
}

