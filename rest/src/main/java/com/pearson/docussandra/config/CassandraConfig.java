
package com.pearson.docussandra.config;

import java.util.Properties;

import org.restexpress.common.exception.ConfigurationException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

/**
 * @author https://github.com/tfredrich
 * @since Dec 20, 2013
 */
public class CassandraConfig {

  private static final String DEFAULT_PORT = "9042";
  private static final String CONTACT_POINTS_PROPERTY = "cassandra.contactPoints";
  private static final String PORT_PROPERTY = "cassandra.port";
  private static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  private static final String DATA_CENTER = "cassandra.dataCenter";
  private static final String READ_CONSISTENCY_LEVEL = "cassandra.readConsistencyLevel";
  private static final String WRITE_CONSISTENCY_LEVEL = "cassandra.writeConsistencyLevel";

  private String[] contactPoints;
  private String keyspace;
  private int port;
  private String dataCenter;
  private ConsistencyLevel readConsistencyLevel;
  private ConsistencyLevel writeConsistencyLevel;

  private Session session;

  public CassandraConfig(Properties p) {
    port = Integer.parseInt(p.getProperty(PORT_PROPERTY, DEFAULT_PORT));
    dataCenter = p.getProperty(DATA_CENTER);
    readConsistencyLevel =
        ConsistencyLevel.valueOf(p.getProperty(READ_CONSISTENCY_LEVEL, "LOCAL_QUORUM"));
    writeConsistencyLevel =
        ConsistencyLevel.valueOf(p.getProperty(WRITE_CONSISTENCY_LEVEL, "LOCAL_QUORUM"));
    keyspace = p.getProperty(KEYSPACE_PROPERTY);

    if (keyspace == null || keyspace.trim().isEmpty()) {
      throw new ConfigurationException(
          "Please define a Cassandra keyspace in property: " + KEYSPACE_PROPERTY);
    }

    String contactPointsCommaDelimited = p.getProperty(CONTACT_POINTS_PROPERTY);

    if (contactPointsCommaDelimited == null || contactPointsCommaDelimited.trim().isEmpty()) {
      throw new ConfigurationException(
          "Please define Cassandra contact points for property: " + CONTACT_POINTS_PROPERTY);
    }

    contactPoints = contactPointsCommaDelimited.split(",\\s*");

    initialize(p);
  }

  /**
   * Sub-classes can override to initialize other properties.
   *
   * @param p Propreties
   */
  protected void initialize(Properties p) {
    // default is to do nothing.
  }

  public String getKeyspace() {
    return keyspace;
  }

  public int getPort() {
    return port;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public ConsistencyLevel getReadConsistencyLevel() {
    return readConsistencyLevel;
  }

  public ConsistencyLevel getWriteConsistencyLevel() {
    return writeConsistencyLevel;
  }

  public Session getSession() {
    if (session == null) {
      session = getCluster().connect(getKeyspace());
    }

    return session;
  }

  protected Cluster getCluster() {
    Builder cb = Cluster.builder();
    cb.addContactPoints(contactPoints);
    cb.withPort(getPort());

    if (getDataCenter() != null) {
      cb.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(getDataCenter()));
    }

    enrichCluster(cb);
    return cb.build();
  }

  /**
   * Sub-classes override this method to do specialized cluster configuration.
   *
   * @param clusterBuilder
   */
  protected void enrichCluster(Builder clusterBuilder) {
    // default is to do nothing.
  }
}
