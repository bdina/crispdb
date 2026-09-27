package cdb

import java.nio.file.Paths
import scala.annotation.tailrec

object dump {
  def run(args: Array[String]): Int = {
    val raw = args.contains("--raw")
    val fileArgs = args.filterNot(_ == "--raw")

    val cdbPath = if (fileArgs.isEmpty || fileArgs(0) == "-") {
      Paths.get("/dev/stdin")
    } else if (fileArgs.length == 1) {
      Paths.get(fileArgs(0))
    } else {
      System.err.println("usage: cdbdump [file] [--raw]")
      return 111
    }

    try {
      val cdb = Cdb(cdbPath)
      val bos = new java.io.BufferedOutputStream(System.out)

      val ARROW = "->".getBytes
      val PLUS = '+'.toByte
      val COMMA = ','.toByte
      val COLON = ':'.toByte
      val NL = '\n'.toByte

      val it = if (raw) cdb.rawIterator else cdb.iterator

      it.foreach { element =>
        val key = element.key
        val klen = key.length.toString.getBytes

        val data = element.data
        val dlen = data.length.toString.getBytes

        bos.write(PLUS)
        bos.write(klen)
        bos.write(COMMA)
        bos.write(dlen)
        bos.write(COLON)
        bos.write(key)
        bos.write(ARROW)
        bos.write(data)
        bos.write(NL)

        bos.flush()
      }
      bos.write(NL)
      bos.flush()
      0
    } catch {
      case t: Throwable =>
        System.err.println(s"cdbdump: error reading CDB: ${t.getMessage}")
        111
    }
  }

  def main(args: Array[String]): Unit = {
    val code = run(args)
    if (code != 0) System.exit(code)
  }
}

object make {
  import java.io.IOException
  import java.nio.file.Path
  import scala.io.Source
  import scala.util.Failure
  import cdb.compression._

  def run(args: Array[String]): Int = {
    var is64 = false
    var codec: CompressionCodec = CompressionCodec.None
    var minSize = 32
    val positional = scala.collection.mutable.ArrayBuffer.empty[String]

    args.foreach { arg =>
      if (arg == "--64") is64 = true
      else if (arg.startsWith("--compress=")) {
        val name = arg.stripPrefix("--compress=")
        CompressionCodec.forName(name) match {
          case Some(c) => codec = c
          case scala.None =>
            System.err.println(s"Unknown compression codec '$name', defaulting to none")
        }
      } else if (arg.startsWith("--min-size=")) {
        val sizeStr = arg.stripPrefix("--min-size=")
        try { minSize = sizeStr.toInt } catch { case _: NumberFormatException => () }
      } else {
        positional += arg
      }
    }

    if (positional.length < 2) {
      System.err.println("usage: cdbmake <cdb_file> <temp_file> [--compress=none|deflate|gzip] [--min-size=N] [--64] [ignoreCdb]")
      111
    } else {
      val cdbPath: Path = Paths.get(positional(0))
      val tempPath: Path = Paths.get(positional(1))

      val ignoreCdb: Option[Cdb] = if (positional.length > 2) {
        try {
          Some(Cdb(Paths.get(positional(2))))
        } catch { case ioe: IOException =>
          System.err.println(s"Couldn't load `ignore' CDB file: ${ioe.getMessage}")
          None
        }
      } else { None }

      val config = CompressionConfig(codec = codec, minBytesThreshold = minSize, onlyIfSmaller = true)
      val makeResult = if (is64) {
        CdbMake64.make(src = Source.fromInputStream(System.in), cdbPath = cdbPath, tempPath = tempPath, config = config, ignoreCdb = ignoreCdb)
      } else {
        CdbMake.make(src = Source.fromInputStream(System.in), cdbPath = cdbPath, tempPath = tempPath, config = config, ignoreCdb = ignoreCdb)
      }

      makeResult match {
        case Failure(t) =>
          System.err.println(s"Couldn't create CDB file: ${t.getMessage}")
          111
        case _ => 0
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val code = run(args)
    if (code != 0) System.exit(code)
  }
}

object get {
  def run(args: Array[String]): Int = {
    val raw = args.contains("--raw")
    val fileArgs = args.filterNot(_ == "--raw")

    val parsed: Option[(java.nio.file.Path, Array[Byte], Int)] = if (fileArgs.length == 1) {
      Some((Paths.get("/dev/stdin"), fileArgs(0).getBytes, 0))
    } else if (fileArgs.length == 2) {
      val firstPath = Paths.get(fileArgs(0))
      if (java.nio.file.Files.exists(firstPath)) {
        Some((firstPath, fileArgs(1).getBytes, 0))
      } else if (fileArgs(1).forall(_.isDigit)) {
        Some((Paths.get("/dev/stdin"), fileArgs(0).getBytes, fileArgs(1).toInt))
      } else {
        Some((firstPath, fileArgs(1).getBytes, 0))
      }
    } else if (fileArgs.length == 3) {
      Some((Paths.get(fileArgs(0)), fileArgs(1).getBytes, fileArgs(2).toInt))
    } else {
      None
    }

    parsed match {
      case None =>
        System.err.println("usage: cdbget [file] <key> [skip] [--raw]")
        111
      case Some((filePath, key, skipCount)) =>
        try {
          val cdb = Cdb(filePath)
          cdb.findstart(key)

          @tailrec
          def find(skipRemaining: Int): Option[Array[Byte]] = {
            val nextOpt = if (raw) cdb.findnextRaw(key) else cdb.findnext(key)
            nextOpt match {
              case None => None
              case Some(data) =>
                if (skipRemaining <= 0) Some(data)
                else find(skipRemaining - 1)
            }
          }

          find(skipCount) match {
            case Some(data) =>
              System.out.write(data)
              System.out.flush()
              0
            case None =>
              100 // DJB standard: key not found
          }
        } catch {
          case t: Throwable =>
            System.err.println(s"cdbget: error: ${t.getMessage}")
            111
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val code = run(args)
    if (code != 0) System.exit(code)
  }
}

