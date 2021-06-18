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
      val slots_ = SlotTable.empty.slots
      var offset = 0
      for (i <- 0 until 256) {
        val pos: Long = (
            table(offset+0) & 0xffL
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

        slots_(i << 1) = pos
        slots_((i << 1) + 1) = len
      }
      SlotTable(slots = slots_)
    }.recover { case ex =>
      println(s"Exception $ex")
      SlotTable.empty
    }.get

  override def close(): Unit = file.tryClose().recover { case ex => println(s"Exception $ex") }

  def findstart(key: Array[Byte]): Unit = state = state.copy(loop = 0)

  def find(key: Array[Byte]): Option[Array[Byte]] = state.synchronized {
    findstart(key)
    findnext(key)
  }

  def findnext(key: Array[Byte]): Option[Array[Byte]] = state.synchronized {
    val slotTable_ = slotTable.slots
    var State(loop_,khash_,hslots_,hpos_,kpos_) = state

    if (slotTable_.isEmpty) return None

    // locate the hash entry (if not yet found)
    if (loop_ == 0) {
      // get hash value for key
      var u = Cdb.hash(key)

      // unpack information for record
      val slot = (u & 255).toInt
      hslots_ = slotTable_((slot << 1) + 1)
      if (hslots_ == 0) {
        state = state.copy(loop=loop_,khash=khash_,hslots=hslots_,hpos=hpos_,kpos=kpos_)
        return None
      }
      hpos_ = slotTable_(slot << 1)

      // store hash value
      khash_ = u

      // locate slot containing this key
      u >>>= 8
      u = u :% hslots_.toInt
      u <<= 3
      kpos_ = hpos_ + u
    }

    // search all hash slots for this key
    while (loop_ < hslots_) {
      // read entry for key from hash slot
      val (hash,pos) = try {
        file.seek(kpos_)
        val hash_ = file.readUnsignedInt()
        val pos_ = file.readUnsignedInt()
        (hash_,pos_)
      } catch { case t: Throwable => (0,0) }

      if (pos == 0L) {
        state = state.copy(loop=loop_,khash=khash_,hslots=hslots_,hpos=hpos_,kpos=kpos_)
        return None
      } else {
        /*
         * advance loop count and key position. wrap key position around to
         * the beginning of the hash slot if we are at the end of the table
         */
        loop_ += 1
        kpos_ += 8L

        if (kpos_ == (hpos_ + (hslots_ << 3))) kpos_ = hpos_

        // ignore this entry if hash values do not match
        if (hash == khash_) {
          // get length of key and data in hash slot entry
          val (hit,data) = try {
            file.seek(pos)
            val klen_ = file.readUnsignedInt()
            val dlen_ = file.readUnsignedInt()
            val key_ = file.readFully(klen_)
            val hit_ = key.sameElements(key_) // read key stored in entry and compare it to key we were given
            val data_ = if (hit_) Some(file.readFully(dlen_)) else None
            (hit_,data_)
          } catch { case t: Throwable => (false,None) }

          // keys match, return the data
          if (hit) {
            state = state.copy(loop=loop_,khash=khash_,hslots=hslots_,hpos=hpos_,kpos=kpos_)
            return data
          }
        }
        // no match; check next slot
      }
    }

    // no more data values for this key
    state = state.copy(loop=loop_,khash=khash_,hslots=hslots_,hpos=hpos_,kpos=kpos_)
    None
  }

  case class Enumerator(in: BufferedInputStream, eod: Int) extends Iterator[Cdb.Element] with AutoCloseable {
    private var pos = INITIAL_POSITION

    override def close(): Unit = in.tryClose()

    override def hasNext: Boolean = pos < eod

    override def next(): Cdb.Element = {
      def read(len: Int): Array[Byte] = {
        @tailrec
        def read_(len: Int, off: Int, data: Array[Byte]): Array[Byte] = {
          if (off < len) {
            var (data,offset) = {
              val (data_,count_) = in.read(off, len - off)
              val offset_ = { off + count_ }
              (data_,offset_)
            }
            read_(len, offset, data)
          } else {
            data
          }
        }
        read_(len, 0, Array.empty[Byte])
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
    def empty = Element(key=Array.empty,data=Array.empty)
  }

  case class SlotTable(slots: Array[Long]) extends AnyVal
  object SlotTable {
    val empty = SlotTable(slots = new Array[Long](256 * 2))
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

  def hash(key: Array[Byte]): Int = {
    import Constants._
    var h = HASH_SEED
    key.foreach { case b =>
      h = h + ((h << 5L) & MASK_32BIT)
      h = (h & MASK_32BIT)
      h = h ^ ((b + HEX_128) & MASK_8BIT).toByte
    }
    (h & MASK_32BIT).toInt
  }

  case object InvalidFormat extends IllegalArgumentException("invalid cdb format")
}
