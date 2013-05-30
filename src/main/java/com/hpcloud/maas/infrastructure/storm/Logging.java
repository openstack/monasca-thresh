package com.hpcloud.maas.infrastructure.storm;

import backtype.storm.task.TopologyContext;

public final class Logging {
  private Logging() {
  }

  public static String categoryFor(TopologyContext ctx) {
    return String.format("%s %s", ctx.getThisComponentId(), ctx.getThisTaskId());
  }
}
