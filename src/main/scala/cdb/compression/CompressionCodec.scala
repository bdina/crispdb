package cdb.compression

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{Deflater, Inflater, GZIPInputStream, GZIPOutputStream}
import scala.util.control.NonFatal

sealed trait CompressionCodec {
  def id: Byte
  def name: String
  def compress(data: Array[Byte]): Array[Byte]
  def decompress(compressed: Array[Byte]): Array[Byte]
  def decompress(compressed: Array[Byte], expectedLength: Int): Array[Byte] = decompress(compressed)
}

object CompressionCodec {
  case object None extends CompressionCodec {
    override val id: Byte = 0x00
    override val name: String = "none"
    override def compress(data: Array[Byte]): Array[Byte] = data
    override def decompress(compressed: Array[Byte]): Array[Byte] = compressed
  }

  case object Deflate extends CompressionCodec {
    override val id: Byte = 0x01
    override val name: String = "deflate"

    override def compress(data: Array[Byte]): Array[Byte] = {
      if (data.isEmpty) return Array.emptyByteArray
      val deflater = new Deflater(Deflater.DEFAULT_COMPRESSION)
      try {
        deflater.setInput(data)
        deflater.finish()
        val out = new ByteArrayOutputStream(math.max(32, data.length / 2))
        val buf = new Array[Byte](4096)
        while (!deflater.finished()) {
          val count = deflater.deflate(buf)
          out.write(buf, 0, count)
        }
        out.toByteArray
      } finally {
        deflater.end()
      }
    }

    override def decompress(compressed: Array[Byte]): Array[Byte] = decompress(compressed, compressed.length * 2)

    override def decompress(compressed: Array[Byte], expectedLength: Int): Array[Byte] = {
      if (compressed.isEmpty) return Array.emptyByteArray
      val inflater = new Inflater()
      try {
        inflater.setInput(compressed)
        val initialCap = if (expectedLength > 0) expectedLength else math.max(32, compressed.length * 2)
        val out = new ByteArrayOutputStream(initialCap)
        val buf = new Array[Byte](4096)
        while (!inflater.finished()) {
          val count = inflater.inflate(buf)
          if (count == 0) {
            if (inflater.needsInput() || inflater.needsDictionary()) {
              throw new java.util.zip.DataFormatException("Truncated or incomplete compressed data")
            }
          } else {
            out.write(buf, 0, count)
          }
        }
        out.toByteArray
      } finally {
        inflater.end()
      }
    }
  }

  case object Gzip extends CompressionCodec {
    override val id: Byte = 0x02
    override val name: String = "gzip"

    override def compress(data: Array[Byte]): Array[Byte] = {
      if (data.isEmpty) return Array.emptyByteArray
      val out = new ByteArrayOutputStream(math.max(32, data.length / 2))
      val gzip = new GZIPOutputStream(out)
      try {
        gzip.write(data)
        gzip.finish()
        gzip.flush()
        out.toByteArray
      } finally {
        gzip.close()
      }
    }

    override def decompress(compressed: Array[Byte]): Array[Byte] = {
      if (compressed.isEmpty) return Array.emptyByteArray
      val in = new ByteArrayInputStream(compressed)
      val gzip = new GZIPInputStream(in)
      try {
        gzip.readAllBytes()
      } finally {
        gzip.close()
      }
    }
  }

  def forName(name: String): Option[CompressionCodec] = name.toLowerCase match {
    case "none" | "raw" => Some(None)
    case "deflate" | "zlib" => Some(Deflate)
    case "gzip" | "gz" => Some(Gzip)
    case _ => scala.None
  }

  def forId(id: Byte): Option[CompressionCodec] = id match {
    case 0x00 => Some(None)
    case 0x01 => Some(Deflate)
    case 0x02 => Some(Gzip)
    case _ => scala.None
  }
}

case class CompressionConfig(
  codec: CompressionCodec = CompressionCodec.None,
  minBytesThreshold: Int = 32,
  onlyIfSmaller: Boolean = true
)

object CompressionConfig {
  val default: CompressionConfig = CompressionConfig(codec = CompressionCodec.None)
  val none: CompressionConfig = default
}

object ValueEnvelope {
  // 4-byte magic signature: 0x00, 'C', 'D', 'Z' (0x00, 0x43, 0x44, 0x5A)
  final val MAGIC_0: Byte = 0x00.toByte
  final val MAGIC_1: Byte = 'C'.toByte
  final val MAGIC_2: Byte = 'D'.toByte
  final val MAGIC_3: Byte = 'Z'.toByte
  final val HEADER_SIZE: Int = 9 // 4 magic + 1 codec ID + 4 uncompressed length LE

  @inline def isFramed(data: Array[Byte]): Boolean = {
    data.length >= HEADER_SIZE &&
      data(0) == MAGIC_0 &&
      data(1) == MAGIC_1 &&
      data(2) == MAGIC_2 &&
      data(3) == MAGIC_3 &&
      CompressionCodec.forId(data(4)).isDefined
  }

  def frame(codec: CompressionCodec, uncompressedLength: Int, compressedPayload: Array[Byte]): Array[Byte] = {
    val target = new Array[Byte](HEADER_SIZE + compressedPayload.length)
    target(0) = MAGIC_0
    target(1) = MAGIC_1
    target(2) = MAGIC_2
    target(3) = MAGIC_3
    target(4) = codec.id
    target(5) = (uncompressedLength & 0xff).toByte
    target(6) = ((uncompressedLength >>> 8) & 0xff).toByte
    target(7) = ((uncompressedLength >>> 16) & 0xff).toByte
    target(8) = ((uncompressedLength >>> 24) & 0xff).toByte
    System.arraycopy(compressedPayload, 0, target, HEADER_SIZE, compressedPayload.length)
    target
  }

  def unframe(data: Array[Byte]): Option[(CompressionCodec, Int, Array[Byte])] = {
    if (!isFramed(data)) scala.None
    else {
      val codecOpt = CompressionCodec.forId(data(4))
      codecOpt.flatMap { codec =>
        val uncompressedLen =
          (data(5) & 0xff) |
            ((data(6) & 0xff) << 8) |
            ((data(7) & 0xff) << 16) |
            ((data(8) & 0xff) << 24)
        val payloadLen = data.length - HEADER_SIZE
        val payload = new Array[Byte](payloadLen)
        System.arraycopy(data, HEADER_SIZE, payload, 0, payloadLen)
        Some((codec, uncompressedLen, payload))
      }
    }
  }

  def compressPayload(data: Array[Byte], config: CompressionConfig): Array[Byte] = {
    if (config.codec == CompressionCodec.None || data.length < config.minBytesThreshold) {
      data
    } else {
      val compressed = config.codec.compress(data)
      val framed = frame(config.codec, data.length, compressed)
      if (config.onlyIfSmaller && framed.length >= data.length) {
        data
      } else {
        framed
      }
    }
  }

  def decompressPayload(data: Array[Byte]): Array[Byte] = {
    unframe(data) match {
      case Some((codec, uncompressedLen, payload)) =>
        try {
          val result = codec.decompress(payload, uncompressedLen)
          if (uncompressedLen >= 0 && result.length == uncompressedLen) result
          else if (uncompressedLen < 0) result
          else {
            // Unexpected length mismatch -> fall back safely to raw bytes
            data
          }
        } catch {
          case NonFatal(_) =>
            // Decompression failed (e.g. random binary data matched magic) -> fall back safely to raw
            data
        }
      case scala.None =>
        // Standard uncompressed CDB record -> return exactly as stored
        data
    }
  }
}
