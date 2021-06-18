package cdb

import java.nio.file.Path

import scala.util.{Failure,Success,Try}

import Constants._

case class CdbMake() {
  import cdb.io._
  import CdbMake._

  import java.io.{BufferedOutputStream,FileOutputStream,RandomAccessFile}

  private final var fp: Option[(RandomAccessFile,BufferedOutputStream)] = None
  private final var state = State.empty

  def start(filepath: Path): Unit = {
    val hashPointers_ = Vector.empty[HashPosition]
    val tableCount_ = Array.fill[Int](256)(0)
    val tableStart_ = Array.fill[Int](256)(0)

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
    val slotTable = new Array[Byte](INITIAL_POSITION)
    for (i <- 0 until 256) {
      val pos_ = state.pos
      val len = tableCount_(i) * 2

      slotTable((i * 8) + 0) = (pos_ & 0xff).byteValue
      slotTable((i * 8) + 1) = ((pos_ >>>  8) & 0xff).byteValue
      slotTable((i * 8) + 2) = ((pos_ >>> 16) & 0xff).byteValue
      slotTable((i * 8) + 3) = ((pos_ >>> 24) & 0xff).byteValue
      slotTable((i * 8) + 4 + 0) = (len & 0xff).byteValue
      slotTable((i * 8) + 4 + 1) = ((len >>>  8) & 0xff).byteValue
      slotTable((i * 8) + 4 + 2) = ((len >>> 16) & 0xff).byteValue
      slotTable((i * 8) + 4 + 3) = ((len >>> 24) & 0xff).byteValue

      var curSlotPointer = tableStart_(i)
      val hashTable = new Array[HashPosition](len)
      for (u <- 0 until tableCount_(i)) {
        val hp = slotPointers(curSlotPointer)
        curSlotPointer += 1

        var where = (hp.hash >>> 8) % len
        while (hashTable(where) != null) {
          where += 1
          if (where == len) {
            where = 0
          }
        }

        hashTable(where) = hp
      }

      for (u <- 0 until len) {
        val hp = hashTable(u)
        if (hp != null) {
          writeLeInt(hashTable(u).hash)
          writeLeInt(hashTable(u).pos.toInt)
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
    }.getOrElse(Failure(IOError("CDB file failed to create")))
  }

  private def writeLeInt(v: Int): Unit = fp.map { case (_,fp) =>
    val bytes = Array(
         (v & 0xff).byteValue
      , ((v >>>  8) & 0xff).byteValue
      , ((v >>> 16) & 0xff).byteValue
      , ((v >>> 24) & 0xff).byteValue
    )
    fp.tryWrite(bytes)
  }

  private def incrementPos(count: Long): Try[Long] = {
    val newpos = state.pos + count
    if (newpos < count)
      Failure(IOError("CDB file is too big."))
    else {
      state = state.copy(pos = newpos)
      Success(state.pos)
    }
  }
}
object CdbMake {
  case class HashPosition(hash: Int = 0, pos: Long = 0)

  sealed trait IllegalArgumentError

  case class IllegalArgument(msg: String) extends IllegalArgumentException(msg) with IllegalArgumentError

  case object InvalidFormat extends IllegalArgumentException("input file not in correct format") with IllegalArgumentError
  case object TruncatedInput extends IllegalArgumentException("input file is truncated") with IllegalArgumentError
  case object InvalidLength extends IllegalArgumentException("length is too big") with IllegalArgumentError

  case class IOError(msg: String) extends java.io.IOException(msg)

  object Tokens {
    final val SEPARATOR = "->".toCharArray
  }

  case class State(hashPointers : Vector[HashPosition], tableCount : Array[Int], tableStart : Array[Int], pos: Long)
  object State {
    def empty = State(hashPointers = Vector.empty, tableCount = Array.empty, tableStart = Array.empty,pos = -1)
  }

  def empty = CdbMake()

  import java.nio.file.Files
  import scala.io.{BufferedSource,Source}
  import scala.util.Using
  def make(dataPath: Path, cdbPath: Path, tempPath: Path, ignoreCdb: Option[Cdb]): Try[Path] = Using.Manager { case use =>
    val is = use(Files.newInputStream(dataPath))
    val src = use(Source.fromInputStream(is))
    val cdbMake = CdbMake.empty
    make(src, cdbPath, tempPath, cdbMake, ignoreCdb).get
  }

  def make(src: BufferedSource, cdbPath: Path, tempPath: Path, cdbMake: CdbMake = CdbMake.empty, ignoreCdb: Option[Cdb] = None): Try[Path] = {
    cdbMake.start(tempPath)

    def parseNewLine(src: Source): Try[Boolean] = if (src.hasNext && src.next() == '\n') Success(true) else {
      println(s"MISSING NEWLINE")
      Failure(InvalidFormat)
    }

    def parseNewRecord(src: Source): Try[Boolean] = {
      val ch = if (src.hasNext) src.next() else ' '
      if ((ch == -1) || (ch == '\n'))
        Success(false)
      else if (ch != '+') {
        println(s"BAD NEW RECORD")
        Failure(InvalidFormat)
      }
      else
        Success(true)
    }

    def parseRecord(src: Source): Try[Int] = {
      def parseLen(separator: Char): Try[Int] = {
        val (err,raw) = src.takeWhile(_ != separator).partition { case ch => (ch < '0') || (ch > '9') }
        if (err.length > 0) {
          println(s"BAD LENGTH")
          Failure(InvalidFormat)
        } else {
          val len = raw.foldLeft (0) ((acc,ch) => acc * 10 + (ch - '0'))
          if (len > 429496720) Failure(InvalidLength) else Success(len)
        }
      }
      def parseVal(len: Int): Try[Array[Byte]] = {
        val (buferr,buf) = src.take(len).partition(_ == -1)
        if (buferr.length > 0) {
          Failure(TruncatedInput)
        } else {
          val raw = new Array[Byte](len)
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

    var write = true // stream closes early when evaluated as loop condition
    while (parseNewRecord(src).getOrElse(false) && write) {
      write = writeRecord.getOrElse(false)
    }

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
