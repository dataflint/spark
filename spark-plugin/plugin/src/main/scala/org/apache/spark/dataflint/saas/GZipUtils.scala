package org.apache.spark.dataflint.saas

import org.apache.commons.io.output.ByteArrayOutputStream

import java.util.zip.GZIPOutputStream

object GZipUtils {
  def compressString(inputString: String): Array[Byte] = {
    val input = inputString.getBytes("UTF-8")
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  def decompressString(compressed: Array[Byte]): String = {
    val bis = new java.io.ByteArrayInputStream(compressed)
    val gzip = new java.util.zip.GZIPInputStream(bis)
    val bos = new java.io.ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)
    var len = gzip.read(buffer)
    while (len > 0) {
      bos.write(buffer, 0, len)
      len = gzip.read(buffer)
    }
    gzip.close()
    bis.close()
    bos.close()
    new String(bos.toByteArray(), "UTF-8")
  }
}
