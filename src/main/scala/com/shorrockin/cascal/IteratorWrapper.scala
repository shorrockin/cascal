package com.shorrockin.cascal

/**
 * utility function used to wrap a java iterator.
 */
class IteratorWrapper[A](iter:java.util.Iterator[A]) {
  def foreach(f: A => Unit): Unit = {
    while(iter.hasNext) f(iter.next)
  }
}