package com.trueaccord.scalapb.compiler

import com.google.protobuf.Descriptors._
import com.google.protobuf.{ByteString => GoogleByteString}
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import scala.collection.JavaConversions._

class FZProtobufGenerator(val params: GeneratorParams) extends FZDescriptorPimps {

  /**
    * Implicit class to add some functions
    */
  class BetterFunctionalPrinter (p: FunctionalPrinter) {
    def addMethod(name: String, mtype: String, params: String, body: String) : FunctionalPrinter = {
      p.add("public " + mtype + " " + name + "(" + params + ")" + " {")
        .indent
        .add(body)
        .outdent
        .add("}")
        .newline
    }

    def addStaticMethod(name: String, mtype: String, params: String, body: String) : FunctionalPrinter = {
      p.add("public static " + mtype + " " + name + "(" + params + ")" + " {")
        .indent
        .add(body)
        .outdent
        .add("}")
        .newline
    }
  }

  implicit def stringToString(s: FunctionalPrinter) = new BetterFunctionalPrinter(s)

  def initRepeatedFldIfNull(field: FieldDescriptor) : String =
    if (field.isRepeated) {s"if (this.${field.getName} == null) this.${field.getName} = new ArrayList<${field.singleJavaTypeName}>();"} else { "" }

  def printEnum(e: EnumDescriptor, p: FunctionalPrinter): FunctionalPrinter = {
    val name = e.nameSymbol
    p
      .add("public enum " + e.getName + " {")
      .indent
      .print(e.getValues) {
        case (v, p) => p.add(
          s"${v.getName}(${v.getNumber}),")
      }
      .add(";")
      .newline
      .add("public final int val;")
      .newline
      .add(s"${e.getName} (int val) { this.val = val; }")
      .newline
      .addStaticMethod("fromValue", s"${e.getName}", "int i", s"for (${e.getName} e : values()) { if (e.val == i) return e; } return null;")
      .outdent
      .add("}")
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
        printer.add(s"out.writeInt32(${field.getNumber}, $valueName.val);")
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
    case FieldDescriptor.JavaType.ENUM => "null"
    case FieldDescriptor.JavaType.INT => "0"
    case FieldDescriptor.JavaType.LONG => "0"
    case FieldDescriptor.JavaType.FLOAT => "0"
    case FieldDescriptor.JavaType.DOUBLE => "0"
    case FieldDescriptor.JavaType.BOOLEAN => "false"
    case _ => "null"
  }

  def defaultValueToString(field: FieldDescriptor) : String = {
    field.getJavaType match {
      case FieldDescriptor.JavaType.ENUM =>
        field.getDefaultValue.asInstanceOf[EnumValueDescriptor].getFullName;
      case FieldDescriptor.JavaType.STRING =>
        "\""+field.getDefaultValue.toString+"\""
      case _ => field.getDefaultValue.toString
    }
  }

  def printMessage(topLevel: Boolean)(message: Descriptor,
                                      printer: FunctionalPrinter): FunctionalPrinter = {
    val requiredFields = message.fields
      .filter(f => !f.isOptional && !f.isRepeated)
      .map(f => s"${f.javaTypeName} ${f.getName}") mkString ", "

    printer
      .newline
      .add("/**")
      .add(s"  * ${message.getName}")
      .add("  */")
      .add(s"public ${if (topLevel) "" else "static "}class ${message.nameSymbol} implements Message, java.io.Serializable {")
      .indent

      .add(s"public static MessageFactory<${message.nameSymbol}> messageFactory;")

      .print(message.fields) {
        case (field, p) => {

          p.add(s"protected ${field.javaTypeName} ${field.getName}${field.initializer};")
            .when(field.isOptional)(p => p.add(s"protected boolean _has_${field.getName};"))
        }
      }

      .newline
      .when(requiredFields.nonEmpty)(
        _
          .add("/**")
          .add(" * Default constructor w/o parameters")
          .add(" */")
          .add(s"protected ${message.nameSymbol} () {")
          .add("}")
          .newline
          .add("/**")
          .add(" * Constructor with required params")
          .add(" */")
          .add(s"public ${message.nameSymbol} ($requiredFields) {" )
          .indent
          .add("// Members initialization")
          .print(message.fields) {
            case (field, p) => { p
              .when(!field.isOptional && !field.isRepeated)(p => p
                .add(s"this.${field.getName} = ${field.getName};")
              )
            }
          }
          .outdent
          .add("}")
          .newline
      )

      .print(message.getEnumTypes)(printEnum)
      .print(message.nestedTypes)(printMessage(topLevel = false))

      // Generate getters and setters
      .print(message.fields) {
        case (field, p) => { p
            .when(field.isRepeated)(p => p
              .addMethod(s"get${field.upperJavaName}", s"${field.singleJavaTypeName}", s"int i", s"return ${field.getName}.get(i);")
              .addMethod(s"get${field.upperJavaName}Iterable", s"Iterable<${field.singleJavaTypeName}>", "",
                  s"return (${field.getName} != null) ? ${field.getName} : Collections.<${field.singleJavaTypeName}>emptyList();")
              .addMethod(s"get${field.upperJavaName}ArrayList", s"ArrayList<${field.singleJavaTypeName}>", "",
                  s"${initRepeatedFldIfNull(field)} return ${field.getName};")
            )
            .when(!field.isRepeated)(p => p
              .addMethod(s"get${field.upperJavaName}", s"${field.javaTypeName}", "",
                if (field.hasDefaultValue) {
                  s"return _has_${field.getName} ? ${field.getName} : ${defaultValueToString(field)};"
                } else {
                  s"return ${field.getName};"
                }
              )
            )
            .when(field.isRepeated || !field.isOptional)(p =>
               p.addMethod(s"set${field.upperJavaName}${if (field.isRepeated) "ArrayList" else ""}",
                 message.nameSymbol,
                  s"${field.javaTypeName} val",
                  s"this.${field.getName} = val; return this;"))
              .when(field.isOptional)(p => p
                .addMethod(s"has${field.upperJavaName}", s"boolean", s"", if (field.hasDefaultValue) "return true;" else s"return _has_${field.getName};")
                  .addMethod(s"clear${field.upperJavaName}", "void", s"", s"this.${field.getName} = ${defaultValue(field)}; _has_${field.getName} = false;")
                  .addMethod(s"set${field.upperJavaName}", message.nameSymbol, s"${field.javaTypeName} val", s"this.${field.getName} = val; _has_${field.getName} = true; return this;"))
              .when(field.isRepeated)(p =>
                p.addMethod(s"add${field.upperJavaName}", message.nameSymbol, s"${field.singleJavaTypeName} item",
                      s"${initRepeatedFldIfNull(field)} this.${field.getName}.add(item); return this;")
                  .addMethod(s"get${field.upperJavaName}Count", "int", "", s"return this.${field.getName} == null ? 0 : this.${field.getName}.size();")
              )
        }
      }
      .newline
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
              .add(s"if (${field.getName} != null) {")
                .indent
                .add(s"for(${field.singleJavaTypeName} q : ${field.getName}) {")
                  .indent
                    .call(generateWriteOne(field, "q"))
                  .outdent
                .add("}")
                .outdent
              .add("}"))
            .when(!field.isRepeated && !field.isOptional)(p => p
              .call(generateWriteOne(field, field.getName)))
        }
      }
      .outdent
      .add("}")

      .newline
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
                p.add(s"set${field.upperJavaName}(${field.singleJavaTypeName}.fromValue(in.readInt32()));")
              case FieldDescriptor.JavaType.MESSAGE =>
                p.add(s"set${field.upperJavaName}(new ${field.singleJavaTypeName}());")
                  .add(s"in.readMessage(${field.getName});")
              case _ =>
                p.add(s"set${field.upperJavaName}(in.read${Types.capitalizedType(field.getType)}());")
            })
              .add("break;"))
          .when(field.isRepeated && field.getJavaType == FieldDescriptor.JavaType.MESSAGE)(p => p
            .add(initRepeatedFldIfNull(field))
            .add(s"{${field.singleJavaTypeName} message = new ${field.singleJavaTypeName}();")
            .add("in.readMessage(message);")
            .add(s"add${field.upperJavaName}(message);}")
            .add("break;")
          )
          .when(field.isRepeated && field.getJavaType != FieldDescriptor.JavaType.MESSAGE)(p => p
            .add(initRepeatedFldIfNull(field))
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
           |    ${message.nameSymbol} message = messageFactory != null ? messageFactory.newInstance() : new ${message.nameSymbol}();
           |    ProtoUtil.messageFromBytes(in, message);
           |    return message;
           |}
           |
           |public byte[] toBytes() throws EncodingException {
           |    return ProtoUtil.messageToBytes(this);
           |}
           |
           |public ${message.nameSymbol} cloneIt() {
           |    ${message.nameSymbol} that = new ${message.nameSymbol}();
           |    that.mergeFrom(this);
           |    return that;
           |}
           """.stripMargin)

      .add("public String toStringImpl(final int indent, String prefix) {")
      .indent
      .add("String res = ProtoUtil.repeatStr(indent, \"  \") + prefix + \"{ " + message.nameSymbol + "\\n\";")
      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(_.call(generateToString(s"if (_has_${field.getName}) ", field, field.getName, field.getName, "")))
            .when(field.isRepeated)(_.call(generateToString(s"if (${field.getName} != null) for(${field.singleJavaTypeName} q : ${field.getName}) { ", field, "q", field.getName + "[]", " }")))
            .when(!field.isRepeated && !field.isOptional)(_.call(generateToString("", field, field.getName, field.getName, "")))
        }
      }
      .add("return res + ProtoUtil.repeatStr(indent, \"  \") + \"}\";")
      .outdent
      .add("}")
      .newline

      .add("public String superToString() { return super.toString(); }")
      .add("public int superHashCode() { return super.hashCode(); }")
      .newline

      .add("@Override")
      .add("public String toString() { return toStringImpl(0,\"\"); }")

      .newline
      .add("@Override")
      .add("public boolean equals(Object o) {")
      .indent
      .add("if (this == o) return true;")
      .add("if (!(o instanceof " + message.nameSymbol + ")) return false;")
      .newline
      .add(message.nameSymbol + " that = (" + message.nameSymbol + ") o;")
      .newline

      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(p => p
              .add(s"if (_has_${field.getName} != that._has_${field.getName}) return false;")
              .indent
              .add(s"if (_has_${field.getName}) {")
                .indent
                .call(generateEquals(field, field.getName, "that." + field.getName))
                .outdent
              .add("}")
              .outdent
              )
            .when(field.isRepeated)(p => p
              .add(s"if (!ProtoUtil.arrayListsEqual(this.${field.getName}, that.${field.getName})) return false;")
//              .add(s"if ((this.${field.getName} == null || this.${field.getName}.size() == 0) != (that.${field.getName} == null || that.${field.getName}.size() == 0)) return false;")
//              .add(s"for(int i = 0; i < ${field.getName}.size(); ++i) {")
//              .indent
//              .call(generateEquals(field, s"this.${field.getName}.get(i)", s"that.${field.getName}.get(i)"))
//              .outdent
//              .add("}")
              )
            .when(!field.isRepeated && !field.isOptional)(p => p
              .call(generateEquals(field, field.getName, "that." + field.getName)))
        }
      }

      .add("return true;")
      .outdent
      .add("}")

      .newline
      .add(s"public ${message.nameSymbol} mergeFrom(${message.nameSymbol} that) {")
      .indent
      .newline

      .print(message.fields) {
        case (field, p) => {
          p
            .when(field.isOptional)(p => p
              .add(s"if (that._has_${field.getName}) set${field.upperJavaName}(that.${field.getName});")
              )
            .when(field.isRepeated)(p => p
              .add(s"if (that.${field.getName} != null) ${field.getName} = that.${field.getName};")
              )
            .when(!field.isRepeated && !field.isOptional)(p => p
              .add(s"${field.getName} = that.${field.getName};")
            )
        }
      }

      .add("return this;")
      .outdent
      .add("}")


      .newline
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
      .newline
  }

  def generateHashcode(field: FieldDescriptor, valueName: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    field.getJavaType match {
      case FieldDescriptor.JavaType.INT =>
        printer.add(s"result = 31 * result + this.${valueName};")
      case FieldDescriptor.JavaType.LONG =>
        printer.add(s"result = 31 * result + (int)(this.${valueName});")
      case FieldDescriptor.JavaType.FLOAT =>
        printer.add(s"result = 31 * result + new Float(this.${valueName}).hashCode();")
      case FieldDescriptor.JavaType.DOUBLE =>
        printer.add(s"result = 31 * result + new Double(this.${valueName}).hashCode();")
      case FieldDescriptor.JavaType.BOOLEAN =>
        printer.add(s"result = 31 * result + (this.${valueName} ? 1 : 0);")
      case FieldDescriptor.JavaType.ENUM =>
        printer.add(s"result = 31 * result + this.${valueName}.val;")
      case _ => {
        printer.add(s"result = 31 * result + this.${valueName}.hashCode();")
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
      case FieldDescriptor.JavaType.FLOAT =>
        printer.add(s"if (${valueName1} != ${valueName2}) return false;")
      case FieldDescriptor.JavaType.DOUBLE =>
        printer.add(s"if (${valueName1} != ${valueName2}) return false;")
      case _ => {
        printer.add(
          s"if ((${valueName1} == null) != (${valueName2} == null)) return false;",
          s"if (${valueName1} != null && !${valueName1}.equals(${valueName2})) return false;")
      }
    }
  }

  def generateToString(prefix: String, field: FieldDescriptor, valueName: String, strValueName: String, suffix: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    def q = "\""
    def space = s"$q${strValueName}=$q"
    field.getJavaType match {
      case FieldDescriptor.JavaType.MESSAGE => {
        printer.add(s"${prefix}res += $valueName.toStringImpl(indent+2, $q${strValueName}=$q) + $q\\n$q;${suffix}")
      } case _ =>
        printer.add(s"${prefix}res += ProtoUtil.repeatStr(indent+1, $q  $q) + $space + $valueName + $q\\n$q;${suffix}")
    }
  }


  val javaImportList = Seq("java.utiфl.ArrayList", "java.util.Collections", "java.io.IOException", "com.ponderingpanda.protobuf.*")

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
      .newline
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

