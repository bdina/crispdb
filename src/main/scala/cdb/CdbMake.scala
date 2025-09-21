package cdb

import java.nio.file.Path

import scala.util.{Failure,Success,Try}

import Constants._

case class CdbMake() {
  import cdb.io._
  import CdbMake._

  import java.io.{BufferedOutputStream,FileOutputStream,RandomAccessFile}

  private final var fp = Option.empty[(RandomAccessFile,BufferedOutputStream)]
  private final var state = State.empty

  def start(filepath: Path): Unit = {
    val hashPointers_ = Vector.empty[HashPosition]
    val tableCount_ = Array.fill(256)(0)
    val tableStart_ = Array.fill(256)(0)

    val filePointer = new RandomAccessFile(filepath.toFile, "rw")

    val pos_ = INITIAL_POSITION

    filePointer.seek(pos_)

    fp = Some((filePointer, new BufferedOutputStream(new FileOutputStream(filePointer.getFD))))

    state = state.copy(hashPointers = hashPointers_, tableCount = tableCount_, tableStart = tableStart_, pos = pos_)
  }

  def add(key: Array[Byte], data: Array[Byte]): Try[Int] = {
    writeLeInt(key.length)
    writeLeInt(data.length)

    fp.map { case (_,fp) => fp.tryWrite(key ++ data) }

    val hash = Cdb.hash(key)

    val tableCount_ = state.tableCount
    tableCount_(hash.toInt & 0xff) += 1
    state = state.copy(hashPointers = state.hashPointers :+ HashPosition(hash.toInt, state.pos), tableCount = tableCount_)

    for {
      _ <- incrementPos(8)
      _ <- incrementPos(key.length)
      _ <- incrementPos(data.length)
    } yield key.length + data.length
  }

  def finish(): Try[Unit] = {
    var curEntry = 0
    val tableStart_ = state.tableStart
    for (i <- 0 until 256) {
      curEntry = curEntry + state.tableCount(i)
      tableStart_(i) = curEntry
    }

    val slotPointers = state.hashPointers.toArray
    state.hashPointers.reverse.view.foreach { case hp =>
      tableStart_(hp.hash & 0xff) -= 1
      slotPointers(tableStart_(hp.hash & 0xff)) = hp
    }

    val tableCount_ = state.tableCount
    val slotTable = new Array[Byte](INITIAL_POSITION) // Pre-allocate with exact size
    for (i <- 0 until 256) {
      val pos_ = state.pos
      val len = tableCount_(i) * 2
      val base = i * 8

      slotTable(base + 0) = (pos_ & 0xff).toByte
      slotTable(base + 1) = ((pos_ >>> 8) & 0xff).toByte
      slotTable(base + 2) = ((pos_ >>> 16) & 0xff).toByte
      slotTable(base + 3) = ((pos_ >>> 24) & 0xff).toByte
      slotTable(base + 4) = (len & 0xff).toByte
      slotTable(base + 5) = ((len >>> 8) & 0xff).toByte
      slotTable(base + 6) = ((len >>> 16) & 0xff).toByte
      slotTable(base + 7) = ((len >>> 24) & 0xff).toByte

      var curSlotPointer = tableStart_(i)
      val hashTable = new Array[HashPosition](len) // Pre-allocate with exact size
      for (u <- 0 until tableCount_(i)) {
        val hp = slotPointers(curSlotPointer)
        curSlotPointer += 1

        var index = (hp.hash >>> 8) % len
        while (hashTable(index) != null && hashTable(index) != HashPosition.empty) {
          index += 1
          if (index == len) {
            index = 0
          }
        }

        hashTable(index) = hp
      }

      for (u <- 0 until len) {
        val hp = hashTable(u)
        if (hp != null && hp != HashPosition.empty) {
          writeLeInt(hp.hash)
          writeLeInt(hp.pos.toInt)
        } else {
          writeLeInt(0)
          writeLeInt(0)
        }
        incrementPos(8)
      }
    }

    fp.map { case (fp,buf) =>
      for {
        _ <- buf.tryFlush()
        _ <- fp.trySeek(0)
        _ <- buf.tryWrite(slotTable)
        _ <- buf.tryFlush()
        result <- fp.tryClose()
      } yield {
        state = state.copy(tableStart=tableStart_,tableCount=tableCount_)
        result
      }
    }.getOrElse(Failure(IOError.FailedToCreate))
  }

  @inline private def writeLeInt(v: Int): Unit = fp.map { case (_,fp) =>
    // Reuse a single array to avoid allocations
    val bytes = Array[Byte](
         (v & 0xff).toByte
      , ((v >>> 8) & 0xff).toByte
      , ((v >>> 16) & 0xff).toByte
      , ((v >>> 24) & 0xff).toByte
    )
    fp.tryWrite(bytes)
  }

  @inline private def incrementPos(count: Long): Try[Long] = {
    val newpos = state.pos + count
    if (newpos < count)
      Failure(IOError.FileSizeExceeded)
    else {
      state = state.copy(pos = newpos)
      Success(state.pos)
    }
  }
}
object CdbMake {
  case class HashPosition(hash: Int, pos: Long)
  object HashPosition {
    val empty = HashPosition(hash = 0, pos = 0L)
  }

  sealed trait IllegalArgumentError
  object IllegalArgumentError {
    case class IllegalArgument(msg: String) extends IllegalArgumentException(msg) with IllegalArgumentError

    case object InvalidFormat extends IllegalArgumentException("input file not in correct format") with IllegalArgumentError
    case object TruncatedInput extends IllegalArgumentException("input file is truncated") with IllegalArgumentError
    case object InvalidLength extends IllegalArgumentException("length is too big") with IllegalArgumentError
  }

  sealed trait IOError
  object IOError {
    case object FileSizeExceeded extends java.io.IOException("CDB file is too big") with IOError
    case object FailedToCreate extends java.io.IOException("CDB file failed to create") with IOError
  }

  object Tokens {
    final val SEPARATOR = "->".toCharArray
  }

  case class State(hashPointers : Vector[HashPosition], tableCount : Array[Int], tableStart : Array[Int], pos: Long)
  object State {
    val empty = State(hashPointers = Vector.empty, tableCount = Array.empty, tableStart = Array.empty, pos = -1)
  }

  def empty = CdbMake()

  import java.nio.file.Files
  import scala.io.{BufferedSource,Source}
  import scala.util.Using
  def make(dataPath: Path, cdbPath: Path, tempPath: Path, ignoreCdb: Option[Cdb]): Try[Path] = Using.Manager {
    case use =>
      val is = use(Files.newInputStream(dataPath))
      val src = use(Source.fromInputStream(is))
      val cdbMake = CdbMake.empty
      make(src, cdbPath, tempPath, cdbMake, ignoreCdb).get
  }

  def make(src: BufferedSource, cdbPath: Path, tempPath: Path, cdbMake: CdbMake = CdbMake.empty, ignoreCdb: Option[Cdb] = None): Try[Path] = {
    cdbMake.start(tempPath)

    def parseNewLine(src: Source): Try[Boolean] = if (src.hasNext && src.next() == '\n') Success(true) else {
      println("MISSING NEWLINE")
      Failure(IllegalArgumentError.InvalidFormat)
    }

    def parseNewRecord(src: Source): Try[Boolean] = {
      val ch = if (src.hasNext) src.next() else ' '
      if ((ch == -1) || (ch == '\n'))
        Success(false)
      else if (ch != '+') {
        println("BAD NEW RECORD")
        Failure(IllegalArgumentError.InvalidFormat)
      }
      else
        Success(true)
    }

    def parseRecord(src: Source): Try[Int] = {
      def parseLen(separator: Char): Try[Int] = {
        val (err,raw) = src.takeWhile(_ != separator).partition { case ch => (ch < '0') || (ch > '9') }
        if (err.length > 0) {
          println("BAD LENGTH")
          Failure(IllegalArgumentError.InvalidFormat)
        } else {
          val len = raw.foldLeft (0) ((acc,ch) => acc * 10 + (ch - '0'))
          if (len > 429496720) Failure(IllegalArgumentError.InvalidLength) else Success(len)
        }
      }
      def parseVal(len: Int): Try[Array[Byte]] = {
        val (buferr,buf) = src.take(len).partition(_ == -1)
        if (buferr.length > 0) {
          Failure(IllegalArgumentError.TruncatedInput)
        } else {
          val raw = Array.fill[Byte](len)(0)
          buf.zipWithIndex.foreach { case (ch,i) => raw(i) = (ch & 0xff).byteValue }
          Success(raw)
        }
      }

      def parseKeyLen(): Try[Int] = parseLen(',')
      def parseKey(klen: Int): Try[Array[Byte]] = parseVal(klen)

      def parseDataLen(): Try[Int] = parseLen(':')
      def parseData(dlen: Int): Try[Array[Byte]] = parseVal(dlen)

      def parseSeparator(): Try[Boolean] = {
        import CdbMake.Tokens._
        val line = src.take(2).toArray
        Success(SEPARATOR.sameElements(line))
      }

      for {
        klen <- parseKeyLen()
        dlen <- parseDataLen()
        key <- parseKey(klen)
        _ <- parseSeparator()
        data <- parseData(dlen)
        add <- cdbMake.add(key, data) if (ignoreCdb.isEmpty || (ignoreCdb.get.find(data) == null))
      } yield add
    }

    def writeRecord = for {
      _ <- parseRecord(src)
      next <- parseNewLine(src)
    } yield next

    def write(src: Source): Boolean = {
      @scala.annotation.tailrec
      def _write(result: Boolean): Boolean =
        if (parseNewRecord(src).getOrElse(false) && result)
          _write(writeRecord.getOrElse(false))
        else
          result

      _write(true)
    }

    write(src)

    for {
      _ <- cdbMake.finish()
      tmp = tempPath.toFile
      cdb = cdbPath.toFile
      _ = tmp.renameTo(cdb)
    } yield {
      println(s"COMPLETED WRITE - $cdb")
      cdbPath
    }
  }
}
