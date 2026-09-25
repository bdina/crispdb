package cdb

object Constants {
  final val INITIAL_POSITION = 2048
}

package object io {
  import java.io.{BufferedInputStream,BufferedOutputStream,RandomAccessFile}

  import scala.util.Try

  implicit class EnhancedRandomAccessFile(val file: RandomAccessFile) extends AnyVal {
    @inline def trySeek(pos: Long): Try[Unit] = Try { file.seek(pos) }
    @inline def safeSeek(pos: Long): Boolean = try { file.seek(pos) ; true } catch { case t: Throwable => false }

    @inline def readFully(len: Int): Array[Byte] = {
      val target = new Array[Byte](len)
      file.readFully(target)
      target
    }

    @inline def tryReadFully(len: Int): Try[Array[Byte]] = Try { file.readFully(len) }

    @inline def tryReadUnsignedByte(): Try[Int] = Try { file.readUnsignedByte() }
    def tryReadUnsignedBytes(len: Int): Try[Array[Int]] = {
      def read = Try {
        val target = new Array[Int](len)
        0.to(len).foreach(i => target(i) = file.readUnsignedByte())
        target
      }

      val pos = file.getFilePointer
      val result = read

      if (result.isFailure) {
        trySeek(pos).recover { case e => println(s"error $e") }
      }

      result
    }

    @inline def readUnsignedInt(): Int =
      (  file.readUnsignedByte()
      | (file.readUnsignedByte() <<  8)
      | (file.readUnsignedByte() << 16)
      | (file.readUnsignedByte() << 24))

    @inline def readUnsignedIntLong(): Long = readUnsignedInt() & 0xFFFFFFFFL

    @inline def readLeLong(): Long = {
      val b0 = file.readUnsignedByte().toLong
      val b1 = file.readUnsignedByte().toLong
      val b2 = file.readUnsignedByte().toLong
      val b3 = file.readUnsignedByte().toLong
      val b4 = file.readUnsignedByte().toLong
      val b5 = file.readUnsignedByte().toLong
      val b6 = file.readUnsignedByte().toLong
      val b7 = file.readUnsignedByte().toLong
      (b0
        | (b1 << 8)
        | (b2 << 16)
        | (b3 << 24)
        | (b4 << 32)
        | (b5 << 40)
        | (b6 << 48)
        | (b7 << 56))
    }

    @inline def tryReadLeLong(): Try[Long] = Try { readLeLong() }

    @inline def tryReadInt(): Try[Int] = {
      val pos = file.getFilePointer
      val result = Try { readUnsignedInt() }

      if (result.isFailure) {
        trySeek(pos).recover { case e => println(s"error $e") }
      }

      result
    }

    @inline def tryWrite(b: Array[Byte]) = Try { file.write(b) }

    @inline def tryClose(): Try[Unit] = Try { file.close() }
  }

  case object ReadError extends Exception("read error")

  implicit class EnhancedBufferedInputStream(val in: BufferedInputStream) extends AnyVal {
    @inline def readLeInt(): Int =
      (  (in.read() & 0xff)
      | ((in.read() & 0xff) <<  8)
      | ((in.read() & 0xff) << 16)
      | ((in.read() & 0xff) << 24))

    @inline def tryReadLeInt(): Try[Int] = Try { readLeInt() }

    @inline def readLeLong(): Long = {
      val b0 = (in.read() & 0xff).toLong
      val b1 = (in.read() & 0xff).toLong
      val b2 = (in.read() & 0xff).toLong
      val b3 = (in.read() & 0xff).toLong
      val b4 = (in.read() & 0xff).toLong
      val b5 = (in.read() & 0xff).toLong
      val b6 = (in.read() & 0xff).toLong
      val b7 = (in.read() & 0xff).toLong
      (b0
        | (b1 << 8)
        | (b2 << 16)
        | (b3 << 24)
        | (b4 << 32)
        | (b5 << 40)
        | (b6 << 48)
        | (b7 << 56))
    }

    @inline def tryReadLeLong(): Try[Long] = Try { readLeLong() }

    @inline def read(off: Int, len: Int): (Array[Byte],Int) = {
      val data = new Array[Byte](len)
      val count = in.read(data, off, len - off)
      (data,count)
    }
    @inline def tryRead(off: Int, len: Int): Try[(Array[Byte],Int)] = {
      import scala.util.{Failure,Success}
      val (data,count) = read(off,len)
      if (count == -1) { Failure(ReadError) } else { Success((data,count)) }
    }

    @inline def trySkip(len: Long): Try[Long] = Try { in.skip(len) }

    @inline def tryClose(): Try[Unit] = Try { in.close() }
  }

  implicit class EnhancedBufferedOutputStream(val out: BufferedOutputStream) extends AnyVal {
    @inline def tryFlush(): Try[Unit] = Try { out.flush() }

    @inline def tryWrite(b: Array[Byte]) = Try { out.write(b) }
    @inline def tryWrite(b: Byte) = Try { out.write(b) }

    @inline def writeLeInt(v: Int): Unit = {
      val bytes = Array[Byte](
           (v & 0xff).toByte
        , ((v >>> 8) & 0xff).toByte
        , ((v >>> 16) & 0xff).toByte
        , ((v >>> 24) & 0xff).toByte
      )
      out.write(bytes)
    }

    @inline def writeLeLong(v: Long): Unit = {
      val bytes = Array[Byte](
           (v & 0xffL).toByte
        , ((v >>> 8) & 0xffL).toByte
        , ((v >>> 16) & 0xffL).toByte
        , ((v >>> 24) & 0xffL).toByte
        , ((v >>> 32) & 0xffL).toByte
        , ((v >>> 40) & 0xffL).toByte
        , ((v >>> 48) & 0xffL).toByte
        , ((v >>> 56) & 0xffL).toByte
      )
      out.write(bytes)
    }
  }

  implicit class UInt(val i: Int) extends AnyVal {
    @inline def :%(divisor: Int): Int = java.lang.Integer.remainderUnsigned(i, divisor)
    @inline def :/(divisor: Int): Int = java.lang.Integer.divideUnsigned(i, divisor)
    @inline def :==(that: Int): Int = java.lang.Integer.compareUnsigned(i,that)
    @inline def :+(that: Int): Long = (i + that) & 0xFFFFFFFFL
    @inline def copyLong: Long = i & 0xFFFFFFFFL
  }
}
package io {
  trait Primitive
  case object Integer extends Primitive {
    final val bytes = 4
  }
  case object LongInteger extends Primitive {
    final val bytes = 8
  }
}

trait CdbLogging {
  import java.util.logging.Logger

  val logger: Logger = Logger.getLogger(this.getClass.getName)
}

object OS {
  final val ONE_MB = 1024*1024
  val runtime = Runtime.getRuntime

  case class Stats(used: Long, free: Long, total: Long, max: Long)

  def shutdownNow(code: Int = 0) = runtime.exit(code)
}

trait SystemManagement {
  import OS._

  def availableProcessors: Int = runtime.availableProcessors

  def memoryCleanup(): Unit = runtime.gc()

  def memoryStats: Stats = {
    val used = (runtime.totalMemory - runtime.freeMemory) / ONE_MB
    val free = runtime.freeMemory / ONE_MB
    val total = runtime.totalMemory / ONE_MB
    val max = runtime.maxMemory / ONE_MB
    Stats(used=used,free=free,total=total,max=max)
  }
}
