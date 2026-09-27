package cdb

import java.io.{BufferedInputStream,RandomAccessFile}
import java.nio.file.Path

import scala.annotation._
import scala.collection._
import scala.util.{Failure,Success}

import Constants._

case class Cdb(filepath: Path) extends immutable.Iterable[Cdb.Element] with AutoCloseable {
  import cdb.Cdb._
  import cdb.io._

  private val file: RandomAccessFile = new RandomAccessFile(filepath.toString, "r")
  private val format: CdbFormat = CdbFormat.detect(file)
  private var state: State = State.empty

  override def iterator: Iterator[Cdb.Element] = Enumerator(filepath, format, raw = false)
  def rawIterator: Iterator[Cdb.Element] = Enumerator(filepath, format, raw = true)

  override def close(): Unit = file.tryClose().recover { case ex => println(s"Exception $ex") }

  @inline final def findstart(key: Array[Byte]): Unit = state = state.copy(loop = 0L)

  @inline final def find(key: Array[Byte]): Option[Array[Byte]] =
    findRaw(key).map(cdb.compression.ValueEnvelope.decompressPayload)

  @inline final def findRaw(key: Array[Byte]): Option[Array[Byte]] = state.synchronized {
    findstart(key)
    findnextRaw(key)
  }

  def findnext(key: Array[Byte]): Option[Array[Byte]] =
    findnextRaw(key).map(cdb.compression.ValueEnvelope.decompressPayload)

  def findnextRaw(key: Array[Byte]): Option[Array[Byte]] = state.synchronized {
    val currentState = state

    // Helper function to initialize hash state if needed
    @inline def initializeHashState(state: State): State = {
      if (state.loop == 0) {
        val u = Cdb.hash(key).copyLong
        val slot = (u & 255L).toInt
        val hslots = format.tableSlots(slot)
        if (hslots == 0) {
          state.copy(hslots = hslots)
        } else {
          val hpos = format.tablePos(slot)
          val khash = u
          val startIndex = ((u >>> 8) % hslots)
          val kpos = hpos + (startIndex * format.slotSizeBytes.toLong)
          state.copy(loop = 0L, khash = khash, hslots = hslots, hpos = hpos, kpos = kpos)
        }
      } else {
        state
      }
    }

    // Helper function to read hash entry from file
    @inline def readHashEntry(pos: Long): (Long, Long) = {
      try {
        format.readSlot(file, pos)
      } catch { case t: Throwable => (0, 0) }
    }

    // Helper function to read and compare key data
    @inline def readKeyData(pos: Long): (Boolean, Option[Array[Byte]]) = {
      try {
        file.seek(pos)
        val klen = file.readUnsignedInt()
        val dlen = file.readUnsignedInt()
        val key_ = file.readFully(klen)
        val hit = key.sameElements(key_)
        val data = if (hit) Some(file.readFully(dlen)) else None
        (hit, data)
      } catch { case t: Throwable => (false, None) }
    }

    // Helper function to advance state to next position
    @inline def advanceState(state: State): State = {
      val newLoop = state.loop + 1
      val newKpos = {
        val nextPos = state.kpos + format.slotSizeBytes.toLong
        val end = state.hpos + (state.hslots * format.slotSizeBytes.toLong)
        if (nextPos == end) state.hpos else nextPos
      }
      state.copy(loop = newLoop, kpos = newKpos)
    }

    // Tail recursive function to search through hash slots
    @tailrec
    def searchSlots(state: State): (State, Option[Array[Byte]]) = {
      val initializedState = initializeHashState(state)

      if (initializedState.hslots == 0) {
        (initializedState, None)
      } else if (initializedState.loop >= initializedState.hslots) {
        (initializedState, None)
      } else {
        val (hash, pos) = readHashEntry(initializedState.kpos)

        if (pos == 0L) {
          (initializedState, None)
        } else {
          val advancedState = advanceState(initializedState)

          if (hash == initializedState.khash) {
            val (hit, data) = readKeyData(pos)
            if (hit) {
              (advancedState, data)
            } else {
              searchSlots(advancedState)
            }
          } else {
            searchSlots(advancedState)
          }
        }
      }
    }

    // Execute the search and update state
    val (finalState, result) = searchSlots(currentState)
    state = finalState
    result
  }

  case class Enumerator(in: BufferedInputStream, eod: Long, raw: Boolean = false) extends Iterator[Cdb.Element] with AutoCloseable {
    private var pos = INITIAL_POSITION.toLong

    override def close(): Unit = in.tryClose()

    override def hasNext: Boolean = pos < eod

    override def next(): Cdb.Element = {
      @inline def read(len: Int): Array[Byte] = {
        if (len == 0) return Array.empty[Byte]
        val data = new Array[Byte](len) // Pre-allocate with exact size
        @tailrec
        def read_(off: Int): Array[Byte] = {
          if (off < len) {
            val count = in.read(data, off, len - off)
            if (count > 0) read_(off + count) else data
          } else {
            data
          }
        }
        read_(0)
      }

      val result = try {
        val klen = in.readLeInt()
        pos += Integer.bytes.toLong
        val dlen = in.readLeInt()
        pos += Integer.bytes.toLong
        val key = read(klen)
        pos += klen.toLong
        val data = read(dlen)
        pos += dlen.toLong
        val payload = if (raw) data else cdb.compression.ValueEnvelope.decompressPayload(data)
        Success(Cdb.Element(key, payload))
      } catch { case t: Throwable => Failure(t) }

      if (result.isFailure) throw Enumerator.NoSuchElementError else result.get
    }
  }
  object Enumerator {
    import java.nio.file.Files

    case object NoSuchElementError extends java.util.NoSuchElementException

    def apply(filepath: Path): Enumerator = apply(filepath, raw = false)

    def apply(filepath: Path, raw: Boolean): Enumerator = {
      val in = new BufferedInputStream(Files.newInputStream(filepath))
      val eod = CdbFormat.detect(filepath).tablePos(0)
      in.skip(INITIAL_POSITION.toLong)
      Enumerator(in, eod, raw)
    }

    def apply(filepath: Path, format: CdbFormat): Enumerator = apply(filepath, format, raw = false)

    def apply(filepath: Path, format: CdbFormat, raw: Boolean): Enumerator = {
      val in = new BufferedInputStream(Files.newInputStream(filepath))
      val eod = format.tablePos(0)
      in.skip(INITIAL_POSITION.toLong)
      Enumerator(in, eod, raw)
    }
  }
}
object Cdb {
  case class Element(key: Array[Byte], data: Array[Byte])
  object Element {
    val empty = Element(key = Array.empty, data = Array.empty)
  }

  case class State(loop: Long, khash: Long, hslots: Long, hpos: Long, kpos: Long)
  object State {
    val empty = State(loop = 0L, khash = 0L, hslots = 0L, hpos = 0L, kpos = 0L)
  }

  object Constants {
    final val HASH_SEED = 5381L

    final val MASK_32BIT = 0x00000000ffffffffL
    final val MASK_8BIT = 0xffL

    final val HEX_128 = 0x100L
  }

  @inline def hash(key: Array[Byte]): Int = {
    import Constants._
    var h = HASH_SEED
    key.foreach { case b =>
      val byteVal = b & 0xff
      h = h + ((h << 5L) & MASK_32BIT)
      h = (h & MASK_32BIT)
      h = h ^ ((byteVal + HEX_128) & MASK_8BIT)
    }
    (h & MASK_32BIT).toInt
  }

  case object InvalidFormat extends IllegalArgumentException("invalid cdb format")
}
