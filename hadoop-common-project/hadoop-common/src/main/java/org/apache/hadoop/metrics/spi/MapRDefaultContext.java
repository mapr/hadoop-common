/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package org.apache.hadoop.metrics.spi;


import org.apache.hadoop.metrics.ContextFactory;

/**
 * Metrics context for recording metrics but not emitting it.<p/>
 *
 * This class is configured by setting ContextFactory attributes which in turn
 * are usually configured through a properties file.  All the attributes are
 * prefixed by the contextName. For example, the properties file might contain:
 * <pre>
 *  myContextName.period=30
 * </pre>
 */
public class MapRDefaultContext extends AbstractMetricsContext {
    
  protected static final String PERIOD_PROPERTY = "period";
    
  /** Creates a new instance of MaprDefaultContext */
  public MapRDefaultContext() {}
    
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
        
    parseAndSetPeriod(PERIOD_PROPERTY);
  }

  /**
   * Emits a metrics record to a file.
   */
  public void emitRecord(String contextName, String recordName, OutputRecord outRec) {
    
  }
}
