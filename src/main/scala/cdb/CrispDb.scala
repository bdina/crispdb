package cdb

import java.nio.file.Paths
import scala.annotation.tailrec

object dump {
  def main(args: Array[String]): Unit = {
    val raw = args.contains("--raw")
    val fileArgs = args.filterNot(_ == "--raw")

    if (fileArgs.length != 1) {
      println("usage: cdb.dump <file> [--raw]")
    } else {
      val cdbFile = fileArgs(0)

      val bos = new java.io.BufferedOutputStream(System.out)

      val ARROW = "->".getBytes
      val PLUS = '+'.toByte
      val COMMA = ','.toByte
      val COLON = ':'.toByte
      val NL = '\n'.toByte

      val cdb = Cdb(Paths.get(cdbFile))
      val it = if (raw) cdb.rawIterator else cdb.iterator

      it.foreach { case element =>
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
    }
  }
}

object make {
  import java.io.IOException
  import java.nio.file.Path
  import scala.io.Source
  import scala.util.Failure
  import cdb.compression._

  def main(args: Array[String]): Unit = {
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
            println(s"Unknown compression codec '$name', defaulting to none")
        }
      } else if (arg.startsWith("--min-size=")) {
        val sizeStr = arg.stripPrefix("--min-size=")
        try { minSize = sizeStr.toInt } catch { case _: NumberFormatException => () }
      } else {
        positional += arg
      }
    }

    if (positional.length < 2) {
      println("usage: cdb.make: <cdb_file> <temp_file> [--compress=none|deflate|gzip] [--min-size=N] [--64] [ignoreCdb]")
    } else {
      val cdbPath: Path = Paths.get(positional(0))
      val tempPath: Path = Paths.get(positional(1))

      val ignoreCdb: Option[Cdb] = if (positional.length > 2) {
        try {
          Some(Cdb(Paths.get(positional(2))))
        } catch { case ioe: IOException =>
          println(s"Couldn't load `ignore' CDB file: ${ioe.getMessage}")
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
          println(s"Couldn't create CDB file: ${t.getMessage}")
        case _ => ()
      }
    }
  }
}

object get {
  def main(args: Array[String]): Unit = {
    val raw = args.contains("--raw")
    val fileArgs = args.filterNot(_ == "--raw")

    if ((fileArgs.length < 2) || (fileArgs.length > 3)) {
      println("usage: cdb.get <file> <key> [skip] [--raw]")
    } else {
      val file = fileArgs(0)
      val key = fileArgs(1).getBytes()
      val skip = if (fileArgs.length == 3) fileArgs(2).toInt + 1 else 1

      val cdb = Cdb(Paths.get(file))
      cdb.findstart(key)

      def find(skip: Int): Array[Byte] = {
        @tailrec
        def _find(skip: Int, data: Array[Byte]): Array[Byte] =
          if (skip <= 0)
            data
          else {
            val nextOpt = if (raw) cdb.findnextRaw(key) else cdb.findnext(key)
            _find(skip - 1, nextOpt.getOrElse(Array.empty[Byte]))
          }

        _find(skip, Array.empty[Byte])
      }

      val data = find(skip)

      System.out.write(data)
      System.out.flush()
    }
  }
}
