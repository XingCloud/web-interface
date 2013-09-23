package com.xingcloud.webinterface.utils;

import com.xingcloud.webinterface.monitor.MonitorInfo;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class GenericQueue<T> {

  public static final GenericQueue<MonitorInfo> SYSTEM_QUEUE_MONITOR_INFO = new GenericQueue<MonitorInfo>();

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
