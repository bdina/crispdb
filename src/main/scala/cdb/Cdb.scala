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
  private var state: State = State.empty

  override def iterator: Iterator[Cdb.Element] = Enumerator(filepath)

  val slotTable: SlotTable =
    file.tryReadFully(len=INITIAL_POSITION).map { case table =>
      val slots_ = new Array[Long](512) // Pre-allocate with exact size
      var offset = 0
      for (i <- 0 until 256) {
        // Inline bit operations for better performance
        val pos: Long = (
            table(offset)   & 0xffL
        | ((table(offset+1) & 0xffL) <<  8)
        | ((table(offset+2) & 0xffL) << 16)
        | ((table(offset+3) & 0xffL) << 24)
        )

        val len: Long = (
            table(offset+4) & 0xffL
        | ((table(offset+5) & 0xffL) <<  8)
        | ((table(offset+6) & 0xffL) << 16)
        | ((table(offset+7) & 0xffL) << 24)
        )

        offset += 8
        val idx = i << 1
        slots_(idx) = pos
        slots_(idx + 1) = len
      }
      SlotTable(slots = slots_)
    }.recover { case ex =>
      println(s"Exception $ex")
      SlotTable.empty
    }.get

  override def close(): Unit = file.tryClose().recover { case ex => println(s"Exception $ex") }

  @inline final def findstart(key: Array[Byte]): Unit = state = state.copy(loop = 0)

  @inline final def find(key: Array[Byte]): Option[Array[Byte]] = state.synchronized {
    findstart(key)
    findnext(key)
  }

  def findnext(key: Array[Byte]): Option[Array[Byte]] = state.synchronized {
    val slotTable_ = slotTable.slots
    val currentState = state

    // Helper function to initialize hash state if needed
    @inline def initializeHashState(state: State): State = {
      if (state.loop == 0) {
        val u = Cdb.hash(key)
        val slot = (u & 255).toInt
        val hslots = slotTable_((slot << 1) + 1)
        if (hslots == 0) {
          state.copy(hslots = hslots)
        } else {
          val hpos = slotTable_(slot << 1)
          val khash = u
          val kpos = hpos + ((u >>> 8) % hslots.toInt << 3)
          state.copy(loop = 0, khash = khash, hslots = hslots, hpos = hpos, kpos = kpos)
        }
      } else {
        state
      }
    }

    // Helper function to read hash entry from file
    @inline def readHashEntry(pos: Long): (Int, Long) = {
      try {
        file.seek(pos)
        val hash = file.readUnsignedInt()
        val entryPos = file.readUnsignedInt()
        (hash, entryPos)
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
        val nextPos = state.kpos + 8L
        if (nextPos == (state.hpos + (state.hslots << 3))) state.hpos else nextPos
      }
      state.copy(loop = newLoop, kpos = newKpos)
    }

    // Tail recursive function to search through hash slots
    @tailrec
    def searchSlots(state: State): (State, Option[Array[Byte]]) = {
      if (slotTable_.isEmpty) {
        (state, None)
      } else {
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
    }

    // Execute the search and update state
    val (finalState, result) = searchSlots(currentState)
    state = finalState
    result
  }

  case class Enumerator(in: BufferedInputStream, eod: Int) extends Iterator[Cdb.Element] with AutoCloseable {
    private var pos = INITIAL_POSITION

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
        pos += Integer.bytes
        val dlen = in.readLeInt()
        pos += Integer.bytes
        val key = read(klen)
        pos += klen
        val data = read(dlen)
        pos += dlen
        Success(Cdb.Element(key, data))
      } catch { case t: Throwable => Failure(t) }

      if (result.isFailure) throw Enumerator.NoSuchElementError else result.get
    }
  }
  object Enumerator {
    import java.nio.file.Files

    case object NoSuchElementError extends java.util.NoSuchElementException

    def apply(filepath: Path): Enumerator = {
      val in = new BufferedInputStream(Files.newInputStream(filepath))
      val eod = in.tryReadLeInt().getOrElse(0)
      in.skip(INITIAL_POSITION - Integer.bytes)
      Enumerator(in, eod)
    }
  }
}
object Cdb {
  case class Element(key: Array[Byte], data: Array[Byte])
  object Element {
    val empty = Element(key = Array.empty, data = Array.empty)
  }

  case class SlotTable(val slots: Array[Long]) extends AnyVal
  object SlotTable {
    val empty = SlotTable(slots = Array.fill(256 * 2)(0L))
  }

  case class State(loop: Int, khash: Int, hslots: Long, hpos: Long, kpos: Long)
  object State {
    val empty = State(loop = 0, khash = 0, hslots = 0, hpos = 0L, kpos = 0L)
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
