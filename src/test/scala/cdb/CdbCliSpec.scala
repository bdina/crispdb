package cdb

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, PrintStream}
import java.nio.file.{Files, Path}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.junit.JUnitRunner
import scala.util.Using

@RunWith(classOf[JUnitRunner])
class CdbCliSpec extends AnyFlatSpec with should.Matchers {

  def withTempDir(f: Path => Unit): Unit = {
    val tempDir = Files.createTempDirectory("cdb-cli-spec")
    try {
      f(tempDir)
    } finally {
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  "CLI cdb.make and cdb.get" should "support --compress and --raw flags" in withTempDir { dir =>
    val cdbFile = dir.resolve("test_cli.cdb").toString
    val tmpFile = dir.resolve("test_cli.cdb.tmp").toString

    val largeText = "CLI compression test payload that compresses well! " * 50
    val input = s"+3,${largeText.getBytes.length}:key->$largeText\n\n"

    // Run cdb.make with stdin
    val oldIn = System.in
    try {
      System.setIn(new ByteArrayInputStream(input.getBytes))
      make.main(Array(cdbFile, tmpFile, "--compress=deflate"))
    } finally {
      System.setIn(oldIn)
    }

    // Verify cdb.get decompresses by default
    val oldOut = System.out
    val getDecompressedOut = new ByteArrayOutputStream()
    try {
      System.setOut(new PrintStream(getDecompressedOut))
      get.main(Array(cdbFile, "key"))
    } finally {
      System.setOut(oldOut)
    }
    val outBytes = getDecompressedOut.toByteArray
    new String(outBytes) should be (largeText)

    // Verify cdb.get with --raw outputs compressed payload
    val getRawOut = new ByteArrayOutputStream()
    try {
      System.setOut(new PrintStream(getRawOut))
      get.main(Array(cdbFile, "key", "--raw"))
    } finally {
      System.setOut(oldOut)
    }
    val rawBytes = getRawOut.toByteArray
    rawBytes.length should be < largeText.getBytes.length
    cdb.compression.ValueEnvelope.isFramed(rawBytes) should be (true)
  }

  "CLI cdb.dump" should "support --raw flag on compressed databases" in withTempDir { dir =>
    val cdbFile = dir.resolve("test_dump.cdb").toString
    val tmpFile = dir.resolve("test_dump.cdb.tmp").toString

    val value = "Repetitive value for dump test! " * 40
    val input = s"+3,${value.getBytes.length}:foo->$value\n\n"

    val oldIn = System.in
    try {
      System.setIn(new ByteArrayInputStream(input.getBytes))
      make.main(Array(cdbFile, tmpFile, "--compress=gzip"))
    } finally {
      System.setIn(oldIn)
    }

    // Test default dump (decompressed)
    val oldOut = System.out
    val dumpDecompressed = new ByteArrayOutputStream()
    try {
      System.setOut(new PrintStream(dumpDecompressed))
      dump.main(Array(cdbFile))
    } finally {
      System.setOut(oldOut)
    }
    dumpDecompressed.toString should include (s"+3,${value.getBytes.length}:foo->$value")

    // Test dump with --raw
    val dumpRaw = new ByteArrayOutputStream()
    try {
      System.setOut(new PrintStream(dumpRaw))
      dump.main(Array(cdbFile, "--raw"))
    } finally {
      System.setOut(oldOut)
    }
    val rawStr = dumpRaw.toString
    rawStr should include ("+3,")
    // In raw mode, dlen must be less than uncompressed length
    rawStr should not include (s":foo->$value")
  }

  "CLI cdb.make" should "produce standard uncompressed CDB when no compression flags are given" in withTempDir { dir =>
    val cdbFile = dir.resolve("test_std.cdb").toString
    val tmpFile = dir.resolve("test_std.cdb.tmp").toString

    val value = "Hello standard CDB"
    val input = s"+3,${value.getBytes.length}:std->$value\n\n"

    val oldIn = System.in
    try {
      System.setIn(new ByteArrayInputStream(input.getBytes))
      make.main(Array(cdbFile, tmpFile))
    } finally {
      System.setIn(oldIn)
    }

    // Verify file format is standard uncompressed
    Using.resource(Cdb(java.nio.file.Paths.get(cdbFile))) { database =>
      val raw = database.findRaw("std".getBytes).get
      new String(raw) should be (value)
      cdb.compression.ValueEnvelope.isFramed(raw) should be (false)
    }
  }
}
