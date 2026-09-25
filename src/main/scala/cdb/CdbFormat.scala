package cdb

import java.io.RandomAccessFile
import java.nio.file.Path

import cdb.Constants._
import cdb.io._

sealed trait CdbFormat {
  def slotSizeBytes: Int
  def headerSizeBytes: Int = INITIAL_POSITION
  def tablePos(table: Int): Long
  def tableSlots(table: Int): Long
  def readSlot(file: RandomAccessFile, pos: Long): (Long, Long)
}

object CdbFormat {
  final val TableCount = 256

  def detect(filepath: Path): CdbFormat = {
    val file = new RandomAccessFile(filepath.toString, "r")
    try {
      detect(file)
    } finally file.close()
  }

  def detect(file: RandomAccessFile): CdbFormat = {
    val header = file.readFully(INITIAL_POSITION)
    val fileSize = file.length()
    V2.tryParse(header, fileSize).getOrElse(V1.parse(header))
  }

  object V1 {
    final val SlotSizeBytes = 8

    def parse(header: Array[Byte]): CdbFormat = {
      val slots = new Array[Long](TableCount * 2)
      var offset = 0
      var i = 0
      while (i < TableCount) {
        val pos =
          (header(offset) & 0xffL) |
            ((header(offset + 1) & 0xffL) << 8) |
            ((header(offset + 2) & 0xffL) << 16) |
            ((header(offset + 3) & 0xffL) << 24)
        val len =
          (header(offset + 4) & 0xffL) |
            ((header(offset + 5) & 0xffL) << 8) |
            ((header(offset + 6) & 0xffL) << 16) |
            ((header(offset + 7) & 0xffL) << 24)
        val idx = i << 1
        slots(idx) = pos
        slots(idx + 1) = len
        offset += 8
        i += 1
      }
      CdbFormatV1(slots)
    }

    final case class CdbFormatV1(slots: Array[Long]) extends CdbFormat {
      override def slotSizeBytes: Int = SlotSizeBytes

      override def tablePos(table: Int): Long = slots(table << 1)

      override def tableSlots(table: Int): Long = slots((table << 1) + 1)

      override def readSlot(file: RandomAccessFile, pos: Long): (Long, Long) = {
        file.seek(pos)
        val hash = file.readUnsignedIntLong()
        val recordPos = file.readUnsignedIntLong()
        (hash, recordPos)
      }
    }
  }

  object V2 {
    final val SlotSizeBytes = 16

    def tryParse(header: Array[Byte], fileSize: Long): Option[CdbFormat] = {
      val tablePos = new Array[Long](TableCount)
      var offset = 0
      var i = 0
      while (i < TableCount) {
        val p = leU64(header, offset)
        tablePos(i) = p
        offset += 8
        i += 1
      }
      if (!isValid(tablePos, fileSize)) None else Some(CdbFormatV2(tablePos, fileSize))
    }

    private def isValid(tablePos: Array[Long], fileSize: Long): Boolean = {
      if (fileSize < INITIAL_POSITION) return false
      if (tablePos.isEmpty) return false
      if (tablePos(0) < INITIAL_POSITION.toLong) return false
      var i = 0
      while (i < TableCount) {
        val p = tablePos(i)
        if (p < 0L || p > fileSize) return false
        if (i > 0 && p < tablePos(i - 1)) return false
        i += 1
      }
      i = 0
      while (i < TableCount - 1) {
        val delta = tablePos(i + 1) - tablePos(i)
        if (delta < 0L) return false
        if (delta % SlotSizeBytes != 0L) return false
        i += 1
      }
      val lastDelta = fileSize - tablePos(TableCount - 1)
      if (lastDelta < 0L) return false
      if (lastDelta % SlotSizeBytes != 0L) return false
      true
    }

    private def leU64(b: Array[Byte], offset: Int): Long = {
      (b(offset) & 0xffL) |
        ((b(offset + 1) & 0xffL) << 8) |
        ((b(offset + 2) & 0xffL) << 16) |
        ((b(offset + 3) & 0xffL) << 24) |
        ((b(offset + 4) & 0xffL) << 32) |
        ((b(offset + 5) & 0xffL) << 40) |
        ((b(offset + 6) & 0xffL) << 48) |
        ((b(offset + 7) & 0xffL) << 56)
    }

    final case class CdbFormatV2(tablePosArr: Array[Long], fileSize: Long) extends CdbFormat {
      override def slotSizeBytes: Int = SlotSizeBytes

      override def tablePos(table: Int): Long = tablePosArr(table)

      override def tableSlots(table: Int): Long = {
        val start = tablePosArr(table)
        val end = if (table == (TableCount - 1)) fileSize else tablePosArr(table + 1)
        (end - start) / SlotSizeBytes
      }

      override def readSlot(file: RandomAccessFile, pos: Long): (Long, Long) = {
        file.seek(pos)
        val hash = file.readLeLong()
        val recordPos = file.readLeLong()
        (hash, recordPos)
      }
    }
  }
}

