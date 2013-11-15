package com.xingcloud.webinterface.utils;

import com.xingcloud.webinterface.monitor.WIEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class GenericQueue<T> {

  public static final GenericQueue<WIEvent> SYSTEM_QUEUE_MONITOR_INFO = new GenericQueue<WIEvent>();

  private BlockingQueue<T> queue = new LinkedBlockingQueue<T>();

  public GenericQueue() {
    super();
  }

  public void putQueue(T t) {
    try {
      queue.put(t);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public T take() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

}
