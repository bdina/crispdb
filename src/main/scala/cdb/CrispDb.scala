package cdb

import java.nio.file.Paths

object dump extends App {
  if (args.length != 1) {
    println("usage: cdb.dump <file>")
  } else {
    val cdbFile = args(0)

    val bos = new java.io.BufferedOutputStream(System.out)

    val ARROW = "->".getBytes
    val PLUS = '+'.toByte
    val COMMA = ','.toByte
    val COLON = ':'.toByte
    val NL = '\n'.toByte

    Cdb(Paths.get(cdbFile)).iterator.foreach { case element =>
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

object make extends App {
  import java.io.IOException
  import java.nio.file.Path
  import scala.io.Source
  import scala.util.Failure

  if (args.length < 2) {
    println("usage: cdb.make: <cdb_file> <temp_file> [ignoreCdb]")
  } else {
    val cdbPath: Path = Paths.get(args(0))
    val tempPath: Path = Paths.get(args(1))

    val ignoreCdb: Option[Cdb] = if ( args.length > 4 ) {
      try {
        Some(Cdb(Paths.get(args(2))))
      } catch { case ioe: IOException =>
        println(s"Couldn't load `ignore' CDB file: ${ioe.getMessage}")
        None
      }
    } else { None }

    CdbMake.make(src=Source.fromInputStream(System.in), cdbPath=cdbPath, tempPath=tempPath, ignoreCdb=ignoreCdb) match {
      case Failure(t) =>
        println(s"Couldn't create CDB file: ${t.getMessage}")
      case _ => ()
    }
  }
}

object get extends App {
  if ((args.length < 2) || (args.length > 3)) {
    println("usage: cdb.get <file> <key> [skip]")
  } else {
    val file = args(0)
    val key = args(1).getBytes()
    val skip = if (args.length == 3) args(2).toInt + 1 else 1

    val cdb = Cdb(Paths.get(file))
    cdb.findstart(key)

    def find(skip: Int): Array[Byte] = {
      @scala.annotation.tailrec
      def _find(skip: Int, data: Array[Byte]): Array[Byte] =
        if (skip <= 0)
          data
        else
          _find(skip - 1,cdb.findnext(key).getOrElse(Array.empty[Byte]))

      _find(skip,Array.empty[Byte])
    }

    println(s"skip => $skip")
    val data = find(skip)

    System.out.write(data)
    System.out.flush()
  }
}
