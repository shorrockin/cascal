package com.shorrockin.cascal.utils

import _root_.scala.io.Source
import java.io.{FileWriter, InputStream, FileOutputStream, File}
import java.util.concurrent.TimeUnit

/**
 * common utility functions that don't fit elsewhere.
 */
object Utils extends Logging {

  /**
   * recursively deletes a directory and all of it's contents
   */
  def delete(directory:File):Boolean = {
    if (directory.exists && directory.isDirectory) {
      val result = directory.listFiles.foldLeft(true) { (left, right) => delete(right) && left }
      val out = directory.delete
      log.debug("deletion attempt on directory: " + directory.getCanonicalPath + " - " + out)
      out
    } else {
      val out = directory.delete
      log.debug("deletion attempt on file: " + directory.getCanonicalPath + " - " + out)
      out
    }
  }


  /**
   * copies the specified resource in the classpath to the specified.
   * resource should generally start with a "/".
   */
  def copy(is:InputStream, file:File):File = {
    var out = new FileOutputStream(file)
    var buf = new Array[Byte](1024)
    var len = 0

    manage(out, is) {
      while (-1 != len) {
        len = is.read(buf, 0, buf.length)
        if (-1 != len) out.write(buf, 0, len)
      }
      out.flush
    }

    file
  }


  /**
   * replaces all instances of the specified token with the specified replacement
   * file in the source file.
   */
  def replace(file:File, replacements:(String, String)*):File = {
    val contents = Source.fromFile(file).getLines().toList.map { (line) =>
      var current = line
      replacements.foreach { (r) => current = current.replace(r._1, r._2) }
      current
    }

    val writer = new FileWriter(file)
    manage(writer) {
      contents.foreach { writer.write(_) }
      writer.flush
    }

    file
  }


  /**
   *  simple function to ignore any error which occurs
   */
  def ignore(f: => Unit):Unit = try { f } catch { case e:Throwable => /* ignore */ }


  /**
   *  performs the close method on the specified object(s) after
   * the specified function has been called
   */
  def manage(closeable:{ def close() }*)(f: => Unit) {
    try { f } finally {
      closeable.foreach { (c) => ignore(c.close()) }
    }
  }

  private val epocBaseMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis)
  private val runBaseNanos = System.nanoTime

  def currentTimeMicros = epocBaseMicros + TimeUnit.NANOSECONDS.toMicros(System.nanoTime-runBaseNanos)

  var COMPENSATE_FOR_LOW_PRECISION_SYSTEM_TIME = System.getProperty("com.shorrockin.cascal.COMPENSATE_FOR_LOW_PRESCISION_SYSTEM_TIME", "true") == "true"

  private var previousNow = System.currentTimeMillis

  /**
   * retuns the current time in micro seconds
   */
  def now = {

    var rc = currentTimeMicros

    // It's very possible the platform can issue repetitive calls to now faster than
    // the the platforms timer can change.
    if( COMPENSATE_FOR_LOW_PRECISION_SYSTEM_TIME ) {
      Utils.synchronized {
        if( rc <= previousNow ) {
          rc = previousNow + 1
        }
        previousNow = rc
      }
    }
    rc
  }
}