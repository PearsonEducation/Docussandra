
package com.pearson.docussandra.cache;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Semaphore used to control access to both the objects that are cached and the caches themselves.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class CacheSynchronizer {

  private static ConcurrentHashMap<Key, Object> semaphoreMap = new ConcurrentHashMap<>();

  // TODO: purge/cleanup map at some point
  public static synchronized Object getLockingObject(String stringKey, Class clazz) {
    Key key = new Key(stringKey, clazz.getCanonicalName());
    Object lock = semaphoreMap.get(key);
    if (lock == null) {
      lock = new Object();
      semaphoreMap.put(key, lock);
    }
    return lock;
  }

  public static synchronized Object getLockingObject(String stringKey, String type) {
    Key key = new Key(stringKey, type);
    Object lock = semaphoreMap.get(key);
    if (lock == null) {
      lock = new Object();
      semaphoreMap.put(key, lock);
    }
    return lock;
  }

  private static class Key {

    private final String keyString;
    private final String type;

    public Key(String keyString, String type) {
      this.keyString = keyString;
      this.type = type;
    }

    public String getKeyString() {
      return keyString;
    }

    public String getType() {
      return type;
    }

    @Override
    public int hashCode() {
      int hash = 5;
      hash = 29 * hash + Objects.hashCode(this.keyString);
      hash = 29 * hash + Objects.hashCode(this.type);
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Key other = (Key) obj;
      if (!Objects.equals(this.keyString, other.keyString)) {
        return false;
      }
      if (!Objects.equals(this.type, other.type)) {
        return false;
      }
      return true;
    }

  }
}
