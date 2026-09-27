package cdb

import java.io.RandomAccessFile
import java.nio.file.{Files, Path, Paths}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.junit.JUnitRunner
import scala.io.Source
import scala.util.Using

@RunWith(classOf[JUnitRunner])
class CdbStandardSpec extends AnyFlatSpec with should.Matchers {

  def withTempCdb(f: (Path, Path) => Unit): Unit = {
    val tempDir = Files.createTempDirectory("cdb-standard-spec")
    val cdbPath = tempDir.resolve("std_test.cdb")
    val tmpPath = tempDir.resolve("std_test.cdb.tmp")
    try {
      f(cdbPath, tmpPath)
    } finally {
      Files.deleteIfExists(cdbPath)
      Files.deleteIfExists(tmpPath)
      Files.deleteIfExists(tempDir)
    }
  }

  "DJB Hash" should "match standard specification test vectors" in {
    // Standard DJB hash with seed 5381: h = ((h << 5) + h) ^ b
    Cdb.hash(Array.emptyByteArray) should be (5381)
    Cdb.hash("a".getBytes) should be (177604)
    Cdb.hash("foo".getBytes) should be (193410979)
    Cdb.hash("bar".getBytes) should be (193415156)
  }

  "Standard CdbMake" should "produce byte-for-byte compliant CDB layout per public domain spec" in withTempCdb { (cdbPath, tmpPath) =>
    val data =
      """+3,5:one->Hello
        |+3,5:two->World
        |
        |""".stripMargin

    val makeResult = CdbMake.make(
      src = Source.fromBytes(data.getBytes),
      cdbPath = cdbPath,
      tempPath = tmpPath,
      cdbMake = CdbMake.empty,
      ignoreCdb = None
    )

    makeResult.isSuccess should be (true)

    // Inspect on-disk binary format directly using RandomAccessFile
    Using.resource(new RandomAccessFile(cdbPath.toFile, "r")) { raf =>
      // 1. Header verification: Exactly 2048 bytes (256 tables * 8 bytes each = (pos: u32_le, len: u32_le))
      raf.length() should be > 2048L

      val headerBytes = new Array[Byte](2048)
      raf.seek(0L)
      raf.readFully(headerBytes)

      def readLeU32(bytes: Array[Byte], offset: Int): Long = {
        (bytes(offset) & 0xffL) |
          ((bytes(offset + 1) & 0xffL) << 8) |
          ((bytes(offset + 2) & 0xffL) << 16) |
          ((bytes(offset + 3) & 0xffL) << 24)
      }

      var totalSlots = 0L
      for (i <- 0 until 256) {
        val pos = readLeU32(headerBytes, i * 8)
        val len = readLeU32(headerBytes, (i * 8) + 4)
        if (len > 0) {
          pos should be >= 2048L
          totalSlots += len
        }
      }

      // We wrote 2 records, each table allocated is 2 * count slots
      totalSlots should be (4L)

      // 2. Record verification at offset 2048:
      // First record: klen=3, dlen=5, key="one", data="Hello"
      raf.seek(2048L)

      def readLeInt(f: RandomAccessFile): Int = {
        val b0 = f.readUnsignedByte()
        val b1 = f.readUnsignedByte()
        val b2 = f.readUnsignedByte()
        val b3 = f.readUnsignedByte()
        b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
      }

      val klen1 = readLeInt(raf)
      val dlen1 = readLeInt(raf)
      val key1 = new Array[Byte](klen1)
      raf.readFully(key1)
      val data1 = new Array[Byte](dlen1)
      raf.readFully(data1)

      klen1 should be (3)
      dlen1 should be (5)
      new String(key1) should be ("one")
      new String(data1) should be ("Hello") // RAW DATA, no headers!

      // Second record: klen=3, dlen=5, key="two", data="World"
      val klen2 = readLeInt(raf)
      val dlen2 = readLeInt(raf)
      val key2 = new Array[Byte](klen2)
      raf.readFully(key2)
      val data2 = new Array[Byte](dlen2)
      raf.readFully(data2)

      klen2 should be (3)
      dlen2 should be (5)
      new String(key2) should be ("two")
      new String(data2) should be ("World") // RAW DATA, no headers!

      // 3. Hash table slot entries: (hash: u32_le, recordPos: u32_le) = 8 bytes per slot
      // Verify total file size equals 2048 + record bytes + (totalSlots * 8)
      val recordBytes = (8 + 3 + 5) + (8 + 3 + 5) // 32 bytes
      val expectedFileSize = 2048L + recordBytes + (totalSlots * 8L)
      raf.length() should be (expectedFileSize)
    }
  }

  "Standard Cdb" should "safely store and read arbitrary binary data without false-positive decompression" in withTempCdb { (cdbPath, tmpPath) =>
    // Construct arbitrary binary payload that happens to start with CDZ magic bytes
    val trickyData = Array[Byte](
      0x00.toByte, 'C'.toByte, 'D'.toByte, 'Z'.toByte,
      0x01.toByte, // Fake Deflate codec
      0xFF.toByte, 0xFF.toByte, 0x00.toByte, 0x00.toByte, // Fake length
      0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte
    )
    val key = "trickyKey".getBytes

    val maker = CdbMake() // Default standard CDB maker
    maker.start(tmpPath)
    maker.add(key, trickyData).isSuccess should be (true)
    maker.finish().isSuccess should be (true)
    Files.move(tmpPath, cdbPath)

    // Read back through standard Cdb
    Using.resource(Cdb(cdbPath)) { cdb =>
      val found = cdb.find(key)
      found shouldBe defined
      // MUST NOT be corrupted by false decompression attempt!
      found.get should be (trickyData)

      val fromRaw = cdb.findRaw(key)
      fromRaw shouldBe defined
      fromRaw.get should be (trickyData)

      val fromIterator = cdb.iterator.find(e => e.key.sameElements(key))
      fromIterator shouldBe defined
      fromIterator.get.data should be (trickyData)
    }
  }

  "Standard Cdb" should "support duplicate keys and nonexistent keys correctly" in withTempCdb { (cdbPath, tmpPath) =>
    val data =
      """+3,6:dup->First!
        |+3,7:dup->Second!
        |+3,6:dup->Third!
        |
        |""".stripMargin

    CdbMake.make(
      src = Source.fromBytes(data.getBytes),
      cdbPath = cdbPath,
      tempPath = tmpPath,
      cdbMake = CdbMake.empty,
      ignoreCdb = None
    ).isSuccess should be (true)

    Using.resource(Cdb(cdbPath)) { cdb =>
      val key = "dup".getBytes
      new String(cdb.find(key).get) should be ("First!")
      new String(cdb.findnext(key).get) should be ("Second!")
      new String(cdb.findnext(key).get) should be ("Third!")
      cdb.findnext(key) should be (None)

      cdb.find("nonexistent".getBytes) should be (None)
    }
  }

  "Standard Cdb64" should "preserve uncompressed records per CDB64 specification" in withTempCdb { (cdbPath, tmpPath) =>
    val data =
      """+4,11:key1->Hello World
        |+4,14:key2->Standard CDB64
        |
        |""".stripMargin

    val makeResult = CdbMake64.make(
      src = Source.fromBytes(data.getBytes),
      cdbPath = cdbPath,
      tempPath = tmpPath,
      cdbMake = CdbMake64.empty,
      ignoreCdb = None
    )
    makeResult.isSuccess should be (true)

    Using.resource(Cdb(cdbPath)) { cdb =>
      new String(cdb.find("key1".getBytes).get) should be ("Hello World")
      new String(cdb.find("key2".getBytes).get) should be ("Standard CDB64")

      val rawEntries = cdb.rawIterator.toVector
      rawEntries.size should be (2)
      new String(rawEntries(0).key) should be ("key1")
      new String(rawEntries(0).data) should be ("Hello World")
    }
  }
}
