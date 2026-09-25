package cdb

import java.nio.file.Path

import scala.annotation.tailrec
import scala.util.{Failure,Success,Try}

import cdb.Constants._

case class CdbMake64() {
  import cdb.io._
  import CdbMake64._

  import java.io.{BufferedOutputStream,FileOutputStream,RandomAccessFile}

  private final var fp = Option.empty[(RandomAccessFile,BufferedOutputStream)]
  private final var state = State.empty

  def start(filepath: Path): Unit = {
    val hashPointers_ = Vector.empty[HashPosition]
    val tableCount_ = Array.fill(256)(0)
    val tableStart_ = Array.fill(256)(0)

    val filePointer = new RandomAccessFile(filepath.toFile, "rw")
    val pos_ = INITIAL_POSITION.toLong
    filePointer.seek(pos_)

    fp = Some((filePointer, new BufferedOutputStream(new FileOutputStream(filePointer.getFD))))

    state =
      state.copy(hashPointers = hashPointers_, tableCount = tableCount_, tableStart = tableStart_, pos = pos_)
  }

  def add(key: Array[Byte], data: Array[Byte]): Try[Int] = {
    fp.foreach { case (_, out) =>
      out.writeLeInt(key.length)
      out.writeLeInt(data.length)
      out.tryWrite(key ++ data)
    }

    val hash = Cdb.hash(key).copyLong

    val tableCount_ = state.tableCount
    tableCount_((hash & 0xffL).toInt) += 1
    state =
      state.copy(hashPointers = state.hashPointers :+ HashPosition(hash, state.pos), tableCount = tableCount_)

    for {
      _ <- incrementPos(8L)
      _ <- incrementPos(key.length.toLong)
      _ <- incrementPos(data.length.toLong)
    } yield key.length + data.length
  }

  def finish(): Try[Unit] = {
    var curEntry = 0
    val tableStart_ = state.tableStart
    var i = 0
    while (i < 256) {
      curEntry = curEntry + state.tableCount(i)
      tableStart_(i) = curEntry
      i += 1
    }

    val slotPointers = state.hashPointers.toArray
    state.hashPointers.reverse.view.foreach { case hp =>
      val idx = (hp.hash & 0xffL).toInt
      tableStart_(idx) -= 1
      slotPointers(tableStart_(idx)) = hp
    }

    val tableCount_ = state.tableCount
    val header = new Array[Byte](INITIAL_POSITION)

    val tablePos = new Array[Long](256)
    i = 0
    while (i < 256) {
      val pos_ = state.pos
      tablePos(i) = pos_
      putLeLong(header, i * 8, pos_)

      val len = tableCount_(i) * 2
      var curSlotPointer = tableStart_(i)

      val hashTable = new Array[HashPosition](len)
      var u = 0
      while (u < tableCount_(i)) {
        val hp = slotPointers(curSlotPointer)
        curSlotPointer += 1

        var index = ((hp.hash >>> 8) % len.toLong).toInt
        while (hashTable(index) != null && hashTable(index) != HashPosition.empty) {
          index += 1
          if (index == len) index = 0
        }
        hashTable(index) = hp
        u += 1
      }

      u = 0
      while (u < len) {
        val hp = hashTable(u)
        fp.foreach { case (_, out) =>
          if (hp != null && hp != HashPosition.empty) {
            out.writeLeLong(hp.hash)
            out.writeLeLong(hp.pos)
          } else {
            out.writeLeLong(0L)
            out.writeLeLong(0L)
          }
        }
        incrementPos(16L)
        u += 1
      }

      i += 1
    }

    fp.map { case (file, out) =>
      for {
        _ <- out.tryFlush()
        _ <- file.trySeek(0)
        _ <- out.tryWrite(header)
        _ <- out.tryFlush()
        result <- file.tryClose()
      } yield {
        state = state.copy(tableStart = tableStart_, tableCount = tableCount_)
        result
      }
    }.getOrElse(Failure(CdbMake.IOError.FailedToCreate))
  }

  @inline private def incrementPos(count: Long): Try[Long] = {
    val newpos = state.pos + count
    if (newpos < count)
      Failure(CdbMake.IOError.FileSizeExceeded)
    else {
      state = state.copy(pos = newpos)
      Success(state.pos)
    }
  }
}

object CdbMake64 {
  case class HashPosition(hash: Long, pos: Long)
  object HashPosition {
    val empty = HashPosition(hash = 0L, pos = 0L)
  }

  case class State(hashPointers: Vector[HashPosition], tableCount: Array[Int], tableStart: Array[Int], pos: Long)
  object State {
    val empty = State(hashPointers = Vector.empty, tableCount = Array.empty, tableStart = Array.empty, pos = -1L)
  }

  def empty: CdbMake64 = CdbMake64()

  private def putLeLong(target: Array[Byte], offset: Int, v: Long): Unit = {
    target(offset + 0) = (v & 0xffL).toByte
    target(offset + 1) = ((v >>> 8) & 0xffL).toByte
    target(offset + 2) = ((v >>> 16) & 0xffL).toByte
    target(offset + 3) = ((v >>> 24) & 0xffL).toByte
    target(offset + 4) = ((v >>> 32) & 0xffL).toByte
    target(offset + 5) = ((v >>> 40) & 0xffL).toByte
    target(offset + 6) = ((v >>> 48) & 0xffL).toByte
    target(offset + 7) = ((v >>> 56) & 0xffL).toByte
  }

  import java.nio.file.Files
  import scala.io.{BufferedSource,Source}
  import scala.util.Using

  def make(dataPath: Path, cdbPath: Path, tempPath: Path, ignoreCdb: Option[Cdb]): Try[Path] = Using.Manager {
    case use =>
      val is = use(Files.newInputStream(dataPath))
      val src = use(Source.fromInputStream(is))
      make(src = src, cdbPath = cdbPath, tempPath = tempPath, ignoreCdb = ignoreCdb).get
  }

  def make(src: BufferedSource, cdbPath: Path, tempPath: Path, cdbMake: CdbMake64 = CdbMake64.empty, ignoreCdb: Option[Cdb] = None)
    : Try[Path] = {

    def parseNewLine(src: Source): Try[Boolean] =
      if (src.hasNext && src.next() == '\n') Success(true) else Failure(CdbMake.IllegalArgumentError.InvalidFormat)

    def parseNewRecord(src: Source): Try[Boolean] = {
      val ch = if (src.hasNext) src.next() else ' '
      if ((ch == -1) || (ch == '\n')) Success(false)
      else if (ch != '+') Failure(CdbMake.IllegalArgumentError.InvalidFormat)
      else Success(true)
    }

    def parseRecord(src: Source): Try[Int] = {
      def parseLen(separator: Char): Try[Int] = {
        val (err, raw) = src.takeWhile(_ != separator).partition { ch => (ch < '0') || (ch > '9') }
        if (err.nonEmpty) Failure(CdbMake.IllegalArgumentError.InvalidFormat)
        else {
          val len = raw.foldLeft(0)((acc, ch) => acc * 10 + (ch - '0'))
          if (len > 429496720) Failure(CdbMake.IllegalArgumentError.InvalidLength) else Success(len)
        }
      }

      def parseVal(len: Int): Try[Array[Byte]] = {
        val (buferr, buf) = src.take(len).partition(_ == -1)
        if (buferr.nonEmpty) Failure(CdbMake.IllegalArgumentError.TruncatedInput)
        else {
          val raw = Array.fill[Byte](len)(0)
          buf.zipWithIndex.foreach { case (ch, i) => raw(i) = (ch & 0xff).byteValue }
          Success(raw)
        }
      }

      def parseSeparator(): Try[Boolean] = {
        import CdbMake.Tokens._
        val line = src.take(2).toArray
        Success(SEPARATOR.sameElements(line))
      }

      for {
        klen <- parseLen(',')
        dlen <- parseLen(':')
        key <- parseVal(klen)
        _ <- parseSeparator()
        data <- parseVal(dlen)
        add <- cdbMake.add(key, data) if (ignoreCdb.isEmpty || (ignoreCdb.get.find(data) == null))
      } yield add
    }

    def writeRecord: Try[Boolean] = for {
      _ <- parseRecord(src)
      next <- parseNewLine(src)
    } yield next

    def write(src: Source): Boolean = {
      @tailrec
      def loop(ok: Boolean): Boolean =
        if (parseNewRecord(src).getOrElse(false) && ok)
          loop(writeRecord.getOrElse(false))
        else ok
      loop(true)
    }

    cdbMake.start(tempPath)
    val result = write(src)

    if (result) {
      for {
        _ <- cdbMake.finish()
        tmp = tempPath.toFile
        cdb = cdbPath.toFile
        _ = tmp.renameTo(cdb)
      } yield cdbPath
    } else Failure(CdbMake.IOError.FailedToCreate)
  }
}

