package com.pearson.docussandra.domain.objects;

/**
 * @author https://github.com/tfredrich
 * @since Jan 8, 2015
 */
public enum IndexType {

  INLINE,
  /**
   * Synchronous, consistent, within the cluster.
   */
  LOCAL,
  /**
   * Asynchronous, eventually-consistent, across the region(s).
   */
  GLOBAL
}
