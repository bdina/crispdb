package cdb

import cdb.compression._
import java.nio.file.{Files, Path}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.junit.JUnitRunner
import scala.io.Source
import scala.util.{Random, Using}

@RunWith(classOf[JUnitRunner])
class CdbCompressionSpec extends AnyFlatSpec with should.Matchers {

  def withTempCdb(f: (Path, Path) => Unit): Unit = {
    val tempDir = Files.createTempDirectory("cdb-compression-spec")
    val cdbPath = tempDir.resolve("compress_test.cdb")
    val tmpPath = tempDir.resolve("compress_test.cdb.tmp")
    try {
      f(cdbPath, tmpPath)
    } finally {
      Files.deleteIfExists(cdbPath)
      Files.deleteIfExists(tmpPath)
      Files.deleteIfExists(tempDir)
    }
  }

  "CompressionCodec.Deflate" should "correctly compress and decompress data" in {
    val original = ("The quick brown fox jumps over the lazy dog. " * 50).getBytes
    val compressed = CompressionCodec.Deflate.compress(original)
    compressed.length should be < original.length

    val decompressed = CompressionCodec.Deflate.decompress(compressed, original.length)
    decompressed should be (original)

    // Empty array
    CompressionCodec.Deflate.compress(Array.emptyByteArray) should be (Array.emptyByteArray)
    CompressionCodec.Deflate.decompress(Array.emptyByteArray) should be (Array.emptyByteArray)
  }

  "CompressionCodec.Gzip" should "correctly compress and decompress data" in {
    val original = ("CrispDB fast constant database with standard compression! " * 50).getBytes
    val compressed = CompressionCodec.Gzip.compress(original)
    compressed.length should be < original.length

    val decompressed = CompressionCodec.Gzip.decompress(compressed)
    decompressed should be (original)

    // Empty array
    CompressionCodec.Gzip.compress(Array.emptyByteArray) should be (Array.emptyByteArray)
    CompressionCodec.Gzip.decompress(Array.emptyByteArray) should be (Array.emptyByteArray)
  }

  "ValueEnvelope" should "correctly frame and unframe payloads" in {
    val raw = "Test Payload for Value Envelope".getBytes
    val compressed = CompressionCodec.Deflate.compress(raw)
    val framed = ValueEnvelope.frame(CompressionCodec.Deflate, raw.length, compressed)

    ValueEnvelope.isFramed(framed) should be (true)
    val unframed = ValueEnvelope.unframe(framed)
    unframed shouldBe defined
    val (codec, uncompressedLen, payload) = unframed.get
    codec should be (CompressionCodec.Deflate)
    uncompressedLen should be (raw.length)
    payload should be (compressed)

    val decompressed = ValueEnvelope.decompressPayload(framed)
    decompressed should be (raw)
  }

  it should "respect minBytesThreshold and onlyIfSmaller" in {
    val shortData = "tiny".getBytes // < 32 bytes
    val config = CompressionConfig(codec = CompressionCodec.Deflate, minBytesThreshold = 32, onlyIfSmaller = true)
    val result1 = ValueEnvelope.compressPayload(shortData, config)
    result1 should be (shortData) // Kept raw!

    // Incompressible random data
    val randomData = new Array[Byte](64)
    Random.nextBytes(randomData)
    val result2 = ValueEnvelope.compressPayload(randomData, config)
    // Compressing 64 bytes of pure random data + 9 bytes envelope will be >= 64 bytes
    // onlyIfSmaller must keep it raw
    result2 should be (randomData)
  }

  "Cdb with Deflate compression (32-bit)" should "compress values and transparently decompress on read" in withTempCdb { (cdbPath, tmpPath) =>
    val largeValue = "Repetitive value data that compresses extremely well! " * 100
    val secondValue = "Second version of large value " + ("ABC" * 100)
    val record1 = s"+4,${largeValue.getBytes.length}:key1->$largeValue\n"
    val record2 = s"+4,${secondValue.getBytes.length}:key1->$secondValue\n"
    val data = record1 + record2 + "\n"

    val config = CompressionConfig(codec = CompressionCodec.Deflate)
    val makeResult = CdbMake.make(
      src = Source.fromBytes(data.getBytes),
      cdbPath = cdbPath,
      tempPath = tmpPath,
      config = config,
      ignoreCdb = None
    )
    makeResult.isSuccess should be (true)

    Using.resource(Cdb(cdbPath)) { cdb =>
      val key1 = "key1".getBytes

      // Transparent decompression
      val v1 = cdb.find(key1)
      v1 shouldBe defined
      new String(v1.get) should be (largeValue)

      val v2 = cdb.findnext(key1)
      v2 shouldBe defined
      new String(v2.get) should startWith ("Second version of large value ")

      // Raw access returns compressed payload (much smaller)
      val rawV1 = cdb.findRaw(key1)
      rawV1 shouldBe defined
      rawV1.get.length should be < largeValue.length
      ValueEnvelope.isFramed(rawV1.get) should be (true)

      // Iterator transparently decompresses
      val entries = cdb.iterator.toVector
      entries.size should be (2)
      new String(entries(0).data) should be (largeValue)

      // Raw iterator returns framed bytes
      val rawEntries = cdb.rawIterator.toVector
      rawEntries.size should be (2)
      rawEntries(0).data.length should be < largeValue.length
    }
  }

  "Cdb with Gzip compression (32-bit)" should "compress and transparently decompress on read" in withTempCdb { (cdbPath, tmpPath) =>
    val largeValue = "GZIP compressed constant database record value! " * 100
    val data = s"+4,${largeValue.length}:gzip->$largeValue\n\n"

    val config = CompressionConfig(codec = CompressionCodec.Gzip)
    val makeResult = CdbMake.make(
      src = Source.fromBytes(data.getBytes),
      cdbPath = cdbPath,
      tempPath = tmpPath,
      config = config,
      ignoreCdb = None
    )
    makeResult.isSuccess should be (true)

    Using.resource(Cdb(cdbPath)) { cdb =>
      val found = cdb.find("gzip".getBytes)
      found shouldBe defined
      new String(found.get) should be (largeValue)

      val raw = cdb.findRaw("gzip".getBytes)
      raw shouldBe defined
      raw.get.length should be < largeValue.length
    }
  }

  "Cdb64 with Deflate compression (64-bit)" should "roundtrip compressed values correctly" in withTempCdb { (cdbPath, tmpPath) =>
    val payload = "CDB64 64-bit offsets with Deflate compression support! " * 80
    val data = s"+5,${payload.length}:cdb64->$payload\n\n"

    val config = CompressionConfig(codec = CompressionCodec.Deflate)
    val makeResult = CdbMake64.make(
      src = Source.fromBytes(data.getBytes),
      cdbPath = cdbPath,
      tempPath = tmpPath,
      config = config,
      ignoreCdb = None
    )
    makeResult.isSuccess should be (true)

    Using.resource(Cdb(cdbPath)) { cdb =>
      val found = cdb.find("cdb64".getBytes)
      found shouldBe defined
      new String(found.get) should be (payload)

      val rawFound = cdb.findRaw("cdb64".getBytes)
      rawFound shouldBe defined
      rawFound.get.length should be < payload.length
    }
  }

  "Adaptive Compression" should "handle mixed databases with small, compressible, and incompressible records" in withTempCdb { (cdbPath, tmpPath) =>
    val smallVal = "short" // < 32 bytes (raw)
    val compressibleVal = "A" * 500 // compressible (deflate)
    val randomBytes = new Array[Byte](128) // incompressible (raw)
    Random.nextBytes(randomBytes)

    val config = CompressionConfig(codec = CompressionCodec.Deflate, minBytesThreshold = 32, onlyIfSmaller = true)
    val maker = CdbMake(config)
    maker.start(tmpPath)
    maker.add("small".getBytes, smallVal.getBytes).isSuccess should be (true)
    maker.add("compressible".getBytes, compressibleVal.getBytes).isSuccess should be (true)
    maker.add("random".getBytes, randomBytes).isSuccess should be (true)
    maker.finish().isSuccess should be (true)
    Files.move(tmpPath, cdbPath)

    Using.resource(Cdb(cdbPath)) { cdb =>
      // Small value remained raw
      cdb.findRaw("small".getBytes).get should be (smallVal.getBytes)
      cdb.find("small".getBytes).get should be (smallVal.getBytes)

      // Compressible value was compressed
      cdb.findRaw("compressible".getBytes).get.length should be < compressibleVal.length
      new String(cdb.find("compressible".getBytes).get) should be (compressibleVal)

      // Random value remained raw because compression would increase size
      cdb.findRaw("random".getBytes).get should be (randomBytes)
      cdb.find("random".getBytes).get should be (randomBytes)
    }
  }
}
