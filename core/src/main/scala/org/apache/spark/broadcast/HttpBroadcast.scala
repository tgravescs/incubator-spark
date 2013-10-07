/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.broadcast

import java.io.{File, FileOutputStream, ObjectInputStream, OutputStream}
import java.net.{Authenticator, PasswordAuthentication, URL, URLConnection, URI}

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import org.apache.spark.{HttpServer, Logging, SecurityManager, SparkEnv}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{Utils, MetadataCleaner, TimeStampedHashSet}


private[spark] class HttpBroadcast[T](@transient var value_ : T, isLocal: Boolean, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {
  
  def value = value_

  def blockId: String = "broadcast_" + id

  HttpBroadcast.synchronized {
    SparkEnv.get.blockManager.putSingle(blockId, value_, StorageLevel.MEMORY_AND_DISK, false)
  }

  if (!isLocal) { 
    HttpBroadcast.write(id, value_)
  }

  // Called by JVM when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(blockId) match {
        case Some(x) => value_ = x.asInstanceOf[T]
        case None => {
          logInfo("Started reading broadcast variable " + id)
          val start = System.nanoTime
          value_ = HttpBroadcast.read[T](id)
          SparkEnv.get.blockManager.putSingle(blockId, value_, StorageLevel.MEMORY_AND_DISK, false)
          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
        }
      }
    }
  }
}

private[spark] class HttpBroadcastFactory extends BroadcastFactory {
  def initialize(isDriver: Boolean) { HttpBroadcast.initialize(isDriver) }

  def newBroadcast[T](value_ : T, isLocal: Boolean, id: Long) =
    new HttpBroadcast[T](value_, isLocal, id)

  def stop() { HttpBroadcast.stop() }
}

private object HttpBroadcast extends Logging {
  private var initialized = false

  private var broadcastDir: File = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536
  private var serverUri: String = null
  private var server: HttpServer = null

  private val files = new TimeStampedHashSet[String]
  private val cleaner = new MetadataCleaner("HttpBroadcast", cleanup)

  private lazy val compressionCodec = CompressionCodec.createCodec()

  def initialize(isDriver: Boolean) {
    synchronized {
      if (!initialized) {
        bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
        compress = System.getProperty("spark.broadcast.compress", "true").toBoolean
        if (isDriver) {
          createServer()
        }
        serverUri = System.getProperty("spark.httpBroadcast.uri")
        initialized = true
      }
    }
  }
  
  def stop() {
    synchronized {
      if (server != null) {
        server.stop()
        server = null
      }
      initialized = false
      cleaner.cancel()
    }
  }

  private def createServer() {
    broadcastDir = Utils.createTempDir(Utils.getLocalDir)
    server = new HttpServer(broadcastDir)
    server.start()
    serverUri = server.uri
    System.setProperty("spark.httpBroadcast.uri", serverUri)
    logInfo("Broadcast server started at " + serverUri)
  }

  def write(id: Long, value: Any) {
    val file = new File(broadcastDir, "broadcast-" + id)
    val out: OutputStream = {
      if (compress) {
        compressionCodec.compressedOutputStream(new FileOutputStream(file))
      } else {
        new FastBufferedOutputStream(new FileOutputStream(file), bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serOut = ser.serializeStream(out)
    serOut.writeObject(value)
    serOut.close()
    files += file.getAbsolutePath
  }

  def read[T](id: Long): T = {
    logDebug("broadcast read server: " +  serverUri + " id: broadcast-"+id)
    val url = serverUri + "/broadcast-" + id

    var uc: URLConnection = null
    if (SecurityManager.isAuthenticationEnabled()) {
      val uri = new URI(url)
      val userInfo = SecurityManager.getHttpUser()  + ":" + SecurityManager.getSecretKey()
      val newuri = new URI(uri.getScheme(), userInfo, uri.getHost(), uri.getPort(), uri.getPath(), 
                           uri.getQuery(), uri.getFragment())

      uc = newuri.toURL().openConnection()
      uc.setAllowUserInteraction(false)
      logDebug("broadcast security enabled")

      // set our own authenticator to properly negotiate user/password
      Authenticator.setDefault(
        new Authenticator(){
          override def getPasswordAuthentication(): PasswordAuthentication = {
            var passAuth : PasswordAuthentication = null
            val userInfo = getRequestingURL().getUserInfo()
            if (userInfo != null) {
              val  parts = userInfo.split(":", 2)
             passAuth = new PasswordAuthentication(parts(0), parts(1).toCharArray())
            }
            return passAuth
          }  
        }
      ); 
    } else {
      logDebug("broadcast not using security")
      uc = new URL(url).openConnection()
    }

    val stream = uc.getInputStream();

    val in = {
      if (compress) {
        compressionCodec.compressedInputStream(stream)
      } else {
        new FastBufferedInputStream(stream, bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  def cleanup(cleanupTime: Long) {
    val iterator = files.internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      val (file, time) = (entry.getKey, entry.getValue)
      if (time < cleanupTime) {
        try {
          iterator.remove()
          new File(file.toString).delete()
          logInfo("Deleted broadcast file '" + file + "'")
        } catch {
          case e: Exception => logWarning("Could not delete broadcast file '" + file + "'", e)
        }
      }
    }
  }
}
