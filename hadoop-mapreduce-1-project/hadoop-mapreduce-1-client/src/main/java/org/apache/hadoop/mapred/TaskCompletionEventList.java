package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class TaskCompletionEventList implements Writable {
  private TaskCompletionEvent[] events;
  private int off;
  private int len;
  public static final TaskCompletionEventList EMPTY =
    new TaskCompletionEventList();

  public TaskCompletionEventList() {
    events = TaskCompletionEvent.EMPTY_ARRAY;
  }

  public TaskCompletionEventList(TaskCompletionEvent[] events, int off,
    int len)
  {
    this.events = events;
    this.off = off;
    this.len = len;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(len);
    for (int i = off; i < off + len; i++) {
      events[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    len = in.readInt();
    events = new TaskCompletionEvent[len];
    for (int i = 0; i < len; i++) {
      final TaskCompletionEvent e = new TaskCompletionEvent();
      e.readFields(in);
      events[i] = e;
    }
  }

  public TaskCompletionEvent[] getArray() {
    return events;
  }

  public int first() {
    return off;
  }

  public int length() {
    return len;
  }

  public int last() {
    return off + len;
  }
}
