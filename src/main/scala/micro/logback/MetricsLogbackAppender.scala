package micro.logback

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.UnsynchronizedAppenderBase

class MetricsLogbackAppender extends UnsynchronizedAppenderBase[ILoggingEvent] {

  override def append(e: ILoggingEvent) = {
    //Send this message to elastic search or REST end point
    messageCount.compute(Thread.currentThread().getName, mergeValue)
    System.out.println(messageCount + " " + e)
  }

  val messageCount = new ConcurrentHashMap[String, AtomicInteger]()
  val mergeValue = new BiFunction[String, AtomicInteger, AtomicInteger] {
    def apply(key: String, currentValue: AtomicInteger) = {
      val nextValue = currentValue match {
        case null => new AtomicInteger(0)
        case _ => currentValue
      }
      nextValue.incrementAndGet()
      nextValue
    }
  }

}
