package cdb

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.OptionValues._
import flatspec._
import matchers._
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.tagobjects.Slow

@RunWith(classOf[JUnitRunner])
class CdbSpec extends AnyFlatSpec with should.Matchers {

  import java.nio.file.Files
  val tempDir = Files.createTempDirectory("cdb-test-spec")
  tempDir.toFile.deleteOnExit

  import java.io.File
  import java.nio.file.Paths
  val cdbPath = Paths.get(s"${tempDir.toString}${File.separator}test.cdb")
  cdbPath.toFile.deleteOnExit

  val cdbSource = Paths.get("src/resources/test.txt")

  "A hash" should "be consistent for the same key" in {
    val k = "foo".getBytes
    val h0 = Cdb.hash(k)

    h0 should be (193410979)

    val h1 = Cdb.hash(k)

    h1 should be (193410979)
  }

  "A Cdb" should "compile a database" in {
    val tempFile = Paths.get(s"${tempDir.toString}${File.separator}test.cdb.tmp")
    tempFile.toFile.deleteOnExit

    val response = CdbMake.make(dataPath=cdbSource, cdbPath=cdbPath, tempPath=tempFile, ignoreCdb=None)
    if (response.isFailure) fail("unable to create cdb") else {
      val path = response.get
      path should be (cdbPath)
    }
  }

  it should "dump contents of a database" in {
    val i = Cdb(cdbPath).iterator
    var m = Map.empty[String,Vector[String]]

    while (i.hasNext) {
      val element = i.next()
      val key = new String(element.key)
      val data = Vector(new String(element.data))
      m = m.updatedWith (key) ( _.map { case v => data :++ v }.orElse { Some(data) } )
    }

    m.size should be (2)
    m.get("one") shouldBe defined
    m.get("two") shouldBe defined
    m("one").reverse should be (Vector("Hello","World"))
    m("two").reverse should be (Vector("Goodbye","Duplicate","Triplicate"))
  }

  it should "fetch values of its keys" in {
    val cdb = Cdb(cdbPath)

    val onekey = "one".getBytes
    val onedata = new String(cdb.find(onekey).getOrElse(Array.empty[Byte]))

    onedata should be ("Hello")

    val twokey = "two".getBytes
    val twodata_first = new String(cdb.find(twokey).getOrElse(Array.empty[Byte]))
    val twodata_second = new String(cdb.findnext(twokey).getOrElse(Array.empty[Byte]))

    twodata_first should be ("Goodbye")
    twodata_second should be ("Duplicate")

    val threekey = "three".getBytes
    cdb.findstart(threekey)
    val threedata = new String(cdb.findnext(threekey).getOrElse(Array.empty[Byte]))

    threedata should be ("")
  }

  val MB_30 = 1_000_000
  val MB_300 = MB_30 * 10
  val GB_3 = MB_300 * 10
  val GB_2_2 = 65_748_600
  val largeCdb = MB_30

  "A Cdb" should s"compile a large database of $largeCdb records and fetch its contents" taggedAs(Slow) in {
    import java.io.{ByteArrayInputStream,ByteArrayOutputStream}
    import scala.io.Source

    val textFile = Paths.get("/tmp/fooey.txt")

    val fos = Files.newOutputStream(textFile)
    val records = (0 until largeCdb).view.map { case i =>
      val (key,data) = (s"key-${i}",s"data-${i}")
      s"+${key.getBytes.length},${data.getBytes.length}:$key->$data\n"
    }.foldLeft (0) { case (acc,record) =>
      fos.write(record.getBytes)
      val acc_ = acc + 1
      if (acc_ % 100_000 == 0 || acc_ == largeCdb) println(s"wrote $acc_ keys")
      acc_
    }
    fos.write("\n".getBytes)

    println(s"CDB source file created - move to write for verification")

    val tempFile = Paths.get("/tmp/fooey.cdb.tmp")
    val cdbFile = Paths.get("/tmp/fooey.cdb")
//    val tempFile = Paths.get(s"${tempDir.toString}${File.separator}test.cdb.tmp")
//    tempFile.toFile.deleteOnExit

    val raw = CdbMake.make(src=Source.createBufferedSource(Files.newInputStream(textFile))
                         , cdbPath=cdbFile
                         , tempPath=tempFile
                         , ignoreCdb=None)
    cdbFile should be (raw.get)

    val cdb = Cdb(cdbFile)

    println(s"CDB file created - move to dump for verification")
    val count = cdb.iterator.zipWithIndex.foldLeft (0) { case (acc,(elem,i)) =>
      val (expectedKey,expectedData) = (s"key-${i}".getBytes,s"data-${i}".getBytes)
      elem.key should be (expectedKey)
      elem.data should be (expectedData)

      if (i % 100_000 == 0) println(s"dumped $i records")
      acc + 1
    }
    println(s"dumped $count records")
    count should be (largeCdb)

    println(s"CDB read verification completed - move to fetch FULL verification")
    val fetched = (0 until largeCdb).view.foldLeft (0) { case (acc,i) =>
      val key = s"key-${i}".getBytes
      val data = s"data-${i}".getBytes
      val next = cdb.find(key)
      if (next.isDefined) {
        next.get should be (data)
      } else fail(s"key $i not found!")

      val notFound = cdb.findnext(key)
      if (notFound.isDefined) fail(s"key ${i} found!")

      if (i % 100_000 == 0 || i == largeCdb) println(s"verified $i keys")
      acc + 1
    }
    println(s"verified $fetched keys")
  }

  it should "read a 64-bit database with offsets beyond 4GiB (sparse file)" in {
    import java.io.RandomAccessFile

    val tempDir2 = Files.createTempDirectory("cdb64-sparse-test")
    tempDir2.toFile.deleteOnExit()
    val cdb64Path = tempDir2.resolve("test64.cdb")
    cdb64Path.toFile.deleteOnExit()

    val key = "k".getBytes
    val data = "v".getBytes

    val recordPos = (4L * 1024L * 1024L * 1024L) + 4096L

    def leIntBytes(v: Int): Array[Byte] =
      Array[Byte](
           (v & 0xff).toByte
        , ((v >>> 8) & 0xff).toByte
        , ((v >>> 16) & 0xff).toByte
        , ((v >>> 24) & 0xff).toByte
      )

    def leLongBytes(v: Long): Array[Byte] =
      Array[Byte](
           (v & 0xffL).toByte
        , ((v >>> 8) & 0xffL).toByte
        , ((v >>> 16) & 0xffL).toByte
        , ((v >>> 24) & 0xffL).toByte
        , ((v >>> 32) & 0xffL).toByte
        , ((v >>> 40) & 0xffL).toByte
        , ((v >>> 48) & 0xffL).toByte
        , ((v >>> 56) & 0xffL).toByte
      )

    val raf = new RandomAccessFile(cdb64Path.toFile, "rw")
    try {
      raf.setLength(0L)
      raf.seek(recordPos)
      raf.write(leIntBytes(key.length))
      raf.write(leIntBytes(data.length))
      raf.write(key)
      raf.write(data)

      val recordSize = 8L + key.length.toLong + data.length.toLong
      val table0Pos = recordPos + recordSize
      val table0Slots = 2
      val table0Bytes = table0Slots * 16L

      val u = Cdb.hash(key).toLong & 0xFFFFFFFFL
      val tableIdx = (u & 0xffL).toInt
      val idx = (((u >>> 8) % table0Slots.toLong).toInt + table0Slots) % table0Slots

      raf.seek(table0Pos)
      var slot = 0
      while (slot < table0Slots) {
        if (slot == idx) {
          raf.write(leLongBytes(u))
          raf.write(leLongBytes(recordPos))
        } else {
          raf.write(leLongBytes(0L))
          raf.write(leLongBytes(0L))
        }
        slot += 1
      }

      val header = new Array[Byte](2048)
      var t = 0
      while (t < 256) {
        val pos = if (t <= tableIdx) table0Pos else table0Pos + table0Bytes
        val off = t * 8
        val bytes = leLongBytes(pos)
        header(off + 0) = bytes(0)
        header(off + 1) = bytes(1)
        header(off + 2) = bytes(2)
        header(off + 3) = bytes(3)
        header(off + 4) = bytes(4)
        header(off + 5) = bytes(5)
        header(off + 6) = bytes(6)
        header(off + 7) = bytes(7)
        t += 1
      }

      raf.seek(0L)
      raf.write(header)

      val finalLen = table0Pos + table0Bytes
      raf.setLength(finalLen)
    } finally raf.close()

    val cdb = Cdb(cdb64Path)
    new String(cdb.find(key).getOrElse(Array.emptyByteArray)) should be ("v")
    cdb.find("missing".getBytes) should be (None)
  }

  it should "compile a 64-bit database and fetch its contents" in {
    val tempDir2 = Files.createTempDirectory("cdb64-roundtrip-test")
    tempDir2.toFile.deleteOnExit()
    val cdbPath2 = tempDir2.resolve("test64.cdb")
    val tmpPath2 = tempDir2.resolve("test64.cdb.tmp")
    cdbPath2.toFile.deleteOnExit()
    tmpPath2.toFile.deleteOnExit()

    val srcPath = Paths.get("src/resources/test.txt")
    val out = CdbMake.make64(dataPath = srcPath, cdbPath = cdbPath2, tempPath = tmpPath2, ignoreCdb = None)
    out.get should be (cdbPath2)

    val cdb = Cdb(cdbPath2)

    val oneKey = "one".getBytes
    new String(cdb.find(oneKey).getOrElse(Array.emptyByteArray)) should be ("Hello")

    val twoKey = "two".getBytes
    val firstTwo = new String(cdb.find(twoKey).getOrElse(Array.emptyByteArray))
    val secondTwo = new String(cdb.findnext(twoKey).getOrElse(Array.emptyByteArray))
    firstTwo should be ("Goodbye")
    secondTwo should be ("Duplicate")

    val keys = cdb.iterator.map(e => new String(e.key)).toVector
    keys.toSet should contain allOf ("one","two")
  }

  it should "optionally write more than 4GiB of real data" taggedAs(Slow) in {
    if (Option(System.getenv("CRISPDB_RUN_LARGE_WRITE")).getOrElse("0") != "1") cancel()

    val tempDir2 = Files.createTempDirectory("cdb64-realwrite-test")
    tempDir2.toFile.deleteOnExit()
    val path = tempDir2.resolve("realwrite.bin")
    path.toFile.deleteOnExit()

    val target = (4L * 1024L * 1024L * 1024L) + (16L * 1024L * 1024L)
    val chunk = new Array[Byte](16 * 1024 * 1024)

    val raf = new java.io.RandomAccessFile(path.toFile, "rw")
    try {
      raf.setLength(0L)
      var written = 0L
      while (written < target) {
        raf.write(chunk)
        written += chunk.length.toLong
      }
      raf.length() should be >= target
    } finally raf.close()
  }
}
