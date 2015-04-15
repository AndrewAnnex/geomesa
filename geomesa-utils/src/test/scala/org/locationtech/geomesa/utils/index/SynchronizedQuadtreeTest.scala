/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.index

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.index.quadtree.Quadtree
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.{Random, Try}

@RunWith(classOf[JUnitRunner])
class SynchronizedQuadtreeTest extends Specification with Logging {

  "SynchronizedQuadtree" should {
    "be thread safe" in {
      skipped("integration")
      val qt = new SynchronizedQuadtree
      val pt = WKTUtils.read("POINT(45 50)")
      val env = pt.getEnvelopeInternal
      val wholeWorld = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").getEnvelopeInternal
      val t1 = new Thread(new Runnable() {
        override def run() = {
          var i = 0
          while (i < 1000) {
            qt.insert(env, pt)
            Thread.sleep(1)
            i += 1
          }
        }
      })
      t1.start()
      var i = 0
      while (i < 1000) {
        qt.query(wholeWorld)
        Thread.sleep(1)
        i += 1
      }
      t1.join()
      success
    }

    "normal quadtree should not be thread safe" in {
      skipped("integration")
      val qt = new Quadtree
      val pt = WKTUtils.read("POINT(45 50)")
      val env = pt.getEnvelopeInternal
      val wholeWorld = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").getEnvelopeInternal
      val t1 = new Thread(new Runnable() {
        override def run() = {
          var i = 0
          while (i < 1000) {
            qt.insert(env, pt)
            Thread.sleep(1)
            i += 1
          }
        }
      })
      t1.start()
      val read = Try({
        var i = 0
        while (i < 1000) {
          qt.query(wholeWorld)
          Thread.sleep(1)
          i += 1
        }
      })
      read should beAFailedTry(beAnInstanceOf[ConcurrentModificationException])
      t1.join()
      success
    }

    "support high throughput" in {

      skipped("integration")

      val qt = new SynchronizedQuadtreeWithMetrics
      val rand = new Random(-75)

      // pre-populate with some data
      val points = (1 to 999999).map { _ =>
        val (x, y) = (rand.nextInt(360) - 180 + rand.nextDouble(), rand.nextInt(180) - 90 + rand.nextDouble())
        WKTUtils.read(s"POINT($x $y)")
      }
      points.foreach(pt => qt.insert(pt.getEnvelopeInternal, pt))
      qt.writeWait.set(0)
      qt.totalWrites.set(0)

      val wholeWorld = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").getEnvelopeInternal

      val endTime = System.currentTimeMillis() + 10000 // 10s

      // 12 writers write a random point every ~5ms
      val writers = (1 to 12).map(_ => new Thread(new Runnable() {
        override def run() = {
          while (System.currentTimeMillis() < endTime) {
            Thread.sleep(rand.nextInt(10))
            val (x, y) = (rand.nextInt(360) - 180 + rand.nextDouble(), rand.nextInt(180) - 90 + rand.nextDouble())
            val pt = WKTUtils.read(s"POINT($x $y)")
            qt.insert(pt.getEnvelopeInternal, pt)
          }
        }
      }))
      // 2 deleters delete a random point every ~100ms
      val deleters = (1 to 2).map(_ => new Thread(new Runnable() {
        override def run() = {
          while (System.currentTimeMillis() < endTime) {
            Thread.sleep(rand.nextInt(200))
            val pt = points(rand.nextInt(points.size))
            qt.remove(pt.getEnvelopeInternal, pt)
          }
        }
      }))
      // 12 readers read the whole world every ~100ms
      val readers = (1 to 12).map(_ => new Thread(new Runnable() {
        override def run() = {
          while (System.currentTimeMillis() < endTime) {
            Thread.sleep(rand.nextInt(200))
            qt.query(wholeWorld)
          }
        }
      }))

      val allThreads = readers ++ writers ++ deleters
      allThreads.foreach(_.start())
      allThreads.foreach(_.join())

      println("Total reads: " + qt.totalReads.get)
      println("Read rate: " + (qt.totalReads.get / 10) + "/s")
      println("Average time waiting for read: " + (qt.readWait.get / qt.totalReads.get) + "ms")
      println("Max time waiting for read: " + qt.maxReadWait.get + "ms")

      println("Total writes: " + qt.totalWrites.get)
      println("Write rate: " + (qt.totalWrites.get / 10) + "/s")
      println("Average time waiting for write: " + (qt.writeWait.get / qt.totalWrites.get) + "ms")
      println("Max time waiting for write: " + qt.maxWriteWait.get + "ms")
      println

      // Average results:

      // Total reads: 1670
      // Read rate: 167/s
      // Average time waiting for read: 13ms
      // Max time waiting for read: 150ms
      // Total writes: 4141
      // Write rate: 414/s
      // Average time waiting for write: 25ms
      // Max time waiting for write: 163ms

      success
    }
  }
}

class SynchronizedQuadtreeWithMetrics extends SynchronizedQuadtree {

  val readWait = new AtomicLong()
  val writeWait = new AtomicLong()
  val maxReadWait = new AtomicLong()
  val maxWriteWait = new AtomicLong()
  val totalReads = new AtomicLong()
  val totalWrites = new AtomicLong()

  override protected[index] def withLock[T](lock: Lock)(fn: => T) = {
    val start = System.currentTimeMillis()
    lock.lock()
    val time = System.currentTimeMillis() - start
    if (lock == readLock) {
      readWait.addAndGet(time)
      totalReads.incrementAndGet()
      var max = maxReadWait.get()
      while (max < time && maxReadWait.compareAndSet(max, time)) {
        max = maxReadWait.get()
      }
    } else {
      writeWait.addAndGet(time)
      totalWrites.incrementAndGet()
      var max = maxWriteWait.get()
      while (max < time && maxWriteWait.compareAndSet(max, time)) {
        max = maxWriteWait.get()
      }
    }
    try {
      fn
    } finally {
      lock.unlock()
    }
  }
}
