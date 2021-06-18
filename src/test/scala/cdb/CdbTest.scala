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
    val onedata = new String(cdb.find(onekey).getOrElse(Array.empty))

    onedata should be ("Hello")

    val twokey = "two".getBytes
    val twodata_first = new String(cdb.find(twokey).getOrElse(Array.empty))
    val twodata_second = new String(cdb.findnext(twokey).getOrElse(Array.empty))

    twodata_first should be ("Goodbye")
    twodata_second should be ("Duplicate")

    val threekey = "three".getBytes
    cdb.findstart(threekey)
    val threedata = new String(cdb.findnext(threekey).getOrElse(Array.empty))

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
}
