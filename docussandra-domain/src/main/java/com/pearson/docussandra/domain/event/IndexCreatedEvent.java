
package com.pearson.docussandra.domain.event;

import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.parent.Identifiable;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.restexpress.plugin.hyperexpress.Linkable;

/**
 * POJO that contains the current status of an indexing action.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexCreatedEvent extends AbstractEvent<Index>
    implements Identifiable, Serializable, Linkable {

  /**
   * UUID for this object.
   */
  private UUID id;

  /**
   * The date that this request to make an index was issued.
   */
  private Date dateStarted;

  /**
   * The last time this status was updated.
   */
  private Date statusLastUpdatedAt;

  /**
   * Estimated time to completion of this index. In seconds. Expect this to be rough.
   */
  private long eta;

  /**
   * Percent complete for this task.
   */
  private double percentComplete;

  /**
   * The requested index that is being created.
   */
  private Index index;

  /**
   * Total number of records that this index will have when complete.
   */
  private long totalRecords;

  /**
   * Number of records that have been indexed.
   */
  private long recordsCompleted;

  /**
   * Fatal error message if an error has occurred in the creation of this index. Will be null if no
   * error has occurred yet. A fatal error will indicate that indexing cannot complete for some
   * reason.
   */
  private String fatalError;

  /**
   * Error messages if a errors have occurred in the creation of this index. Will be null if no
   * errors have occurred yet. As opposed to a fatalError, these errors are more like warnings; the
   * index as a whole will still complete.
   */
  private List<String> errors;

  /**
   * Default constructor for JSON deserialization only.
   */
  public IndexCreatedEvent() {
    super(null);
  }

  /**
   * Constructor.
   *
   * @param id
   * @param dateStarted
   * @param statusLastUpdatedAt
   * @param index
   * @param totalRecords
   * @param recordsCompleted
   */
  public IndexCreatedEvent(UUID id, Date dateStarted, Date statusLastUpdatedAt, Index index,
      long totalRecords, long recordsCompleted) {
    super(index);
    this.id = id;
    this.dateStarted = dateStarted;
    this.statusLastUpdatedAt = statusLastUpdatedAt;
    this.index = index;
    this.totalRecords = totalRecords;
    this.recordsCompleted = recordsCompleted;
  }

  /**
   * Computes any calculated getFields.
   */
  public void calculateValues() {
    calculatePercentComplete();
    calculateEta();
  }

  /**
   * Returns if this index is done indexing or not.
   *
   * @return
   */
  public boolean isDoneIndexing() {
    if (getIndex() != null) {
      return getIndex().isActive();
    } else {
      return false;// if for some bizarre reason we don't have an index here, we can't say that it's
                   // done
    }
  }

  /**
   * Calculates out what percent complete this operation is. If more records get added during the
   * operation, the percent complete could decrease instead of climb.
   */
  private void calculatePercentComplete() {
    if (getTotalRecords() == 0) {
      percentComplete = 100;
    } else if (getRecordsCompleted() == 0) {
      percentComplete = 0;
    } else {
      percentComplete =
          (double) ((double) getRecordsCompleted() / (double) getTotalRecords()) * 100d;
    }
  }

  private void calculateEta() {
    long duration = new Date().getTime() - this.getDateStarted().getTime();
    if (getTotalRecords() == 0) {
      eta = 0;// we are functionally done
    } else if (duration == 0 || fatalError != null)// nothing to go off of OR we have an fatalError
                                                   // and it's never going to finish
    {
      eta = -1;
    } else {
      long recordsProcessed = getRecordsCompleted();
      long recordsRemaining = getTotalRecords() - getRecordsCompleted();
      // lets get duration in seconds
      double durationDouble = (double) duration / 1000d;
      double doubleEta = (durationDouble / recordsProcessed) * recordsRemaining;// might need to be
                                                                                // recordsProcessed/durationDouble
      eta = (long) doubleEta;
    }
  }

  public UUID getUuid() {
    return id;
  }

  public void setUuid(UUID id) {
    this.id = id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public Identifier getId() {
    return new Identifier(getIndex().getDatabaseName(), getIndex().getTableName(),
        getIndex().getName(), id);
  }

  /**
   * The date that this request to make an index was issued.
   *
   * @return the dateStarted
   */
  public Date getDateStarted() {
    return dateStarted;
  }

  /**
   * The date that this request to make an index was issued.
   *
   * @param dateStarted the dateStarted to set
   */
  public void setDateStarted(Date dateStarted) {
    this.dateStarted = dateStarted;
  }

  /**
   * The last time this status was updated.
   *
   * @return the statusLastUpdatedAt
   */
  public Date getStatusLastUpdatedAt() {
    return statusLastUpdatedAt;
  }

  /**
   * The last time this status was updated.
   *
   * @param statusLastUpdatedAt the statusLastUpdatedAt to set
   */
  public void setStatusLastUpdatedAt(Date statusLastUpdatedAt) {
    this.statusLastUpdatedAt = statusLastUpdatedAt;
  }

  /**
   * Estimated time to completion of this index.
   *
   * @return the eta
   */
  public long getEta() {
    return eta;
  }

  /**
   * The requested index that is being created.
   *
   * @return the index
   */
  public Index getIndex() {
    return index;
  }

  /**
   * The requested index that is being created.
   *
   * @param index the index to set
   */
  public void setIndex(Index index) {
    this.index = index;
  }

  /**
   * Total number of records that this index will have when complete.
   *
   * @return the totalRecords
   */
  public long getTotalRecords() {
    return totalRecords;
  }

  /**
   * Total number of records that this index will have when complete.
   *
   * @param totalRecords the totalRecords to set
   */
  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  /**
   * Number of records that have been indexed.
   *
   * @return the recordsCompleted
   */
  public long getRecordsCompleted() {
    return recordsCompleted;
  }

  /**
   * Number of records that have been indexed.
   *
   * @param recordsCompleted the recordsCompleted to set
   */
  public void setRecordsCompleted(long recordsCompleted) {
    this.recordsCompleted = recordsCompleted;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 41 * hash + Objects.hashCode(this.id);
    hash = 41 * hash + Objects.hashCode(this.dateStarted);
    hash = 41 * hash + Objects.hashCode(this.statusLastUpdatedAt);
    hash = 41 * hash + (int) (this.eta ^ (this.eta >>> 32));
    hash = 41 * hash + (int) (Double.doubleToLongBits(this.percentComplete)
        ^ (Double.doubleToLongBits(this.percentComplete) >>> 32));
    hash = 41 * hash + Objects.hashCode(this.index);
    hash = 41 * hash + (int) (this.totalRecords ^ (this.totalRecords >>> 32));
    hash = 41 * hash + (int) (this.recordsCompleted ^ (this.recordsCompleted >>> 32));
    hash = 41 * hash + Objects.hashCode(this.fatalError);
    hash = 41 * hash + Objects.hashCode(this.errors);
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
    final IndexCreatedEvent other = (IndexCreatedEvent) obj;
    if (!Objects.equals(this.id, other.id)) {
      return false;
    }
    if (!Objects.equals(this.dateStarted, other.dateStarted)) {
      return false;
    }
    if (!Objects.equals(this.statusLastUpdatedAt, other.statusLastUpdatedAt)) {
      return false;
    }
    if (this.eta != other.eta) {
      return false;
    }
    if (Double.doubleToLongBits(this.percentComplete) != Double
        .doubleToLongBits(other.percentComplete)) {
      return false;
    }
    if (!Objects.equals(this.index, other.index)) {
      return false;
    }
    if (this.totalRecords != other.totalRecords) {
      return false;
    }
    if (this.recordsCompleted != other.recordsCompleted) {
      return false;
    }
    if (!Objects.equals(this.fatalError, other.fatalError)) {
      return false;
    }
    if (!Objects.equals(this.errors, other.errors)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "IndexCreatedEvent{" + "id=" + id + ", dateStarted=" + dateStarted
        + ", statusLastUpdatedAt=" + statusLastUpdatedAt + ", eta=" + eta + ", percentComplete="
        + percentComplete + ", index=" + index + ", totalRecords=" + totalRecords
        + ", recordsCompleted=" + recordsCompleted + ", fatalError=" + fatalError + ", errors="
        + errors + '}';
  }

  /**
   * Percent complete for this task.
   *
   * @return the percentComplete
   */
  public double getPercentComplete() {
    return percentComplete;
  }

  /**
   * Error message if an fatalError has occurred in the creation of this index. Will be null if no
   * fatalError has occurred yet.
   *
   * @return the fatalError
   */
  public String getFatalError() {
    return fatalError;
  }

  /**
   * Error message if an fatalError has occurred in the creation of this index. Will be null if no
   * fatalError has occurred yet.
   *
   * @param fatalError the fatalError to set
   */
  public void setFatalError(String fatalError) {
    this.fatalError = fatalError;
  }

  /**
   * Error messages if a errors have occurred in the creation of this index. Will be null if no
   * errors have occurred yet. As opposed to a fatalError, these errors are more like warnings; the
   * index as a whole will still complete.
   *
   * @return the errors
   */
  public List<String> getErrors() {
    return errors;
  }

  /**
   * Error messages if a errors have occurred in the creation of this index. Will be null if no
   * errors have occurred yet. As opposed to a fatalError, these errors are more like warnings; the
   * index as a whole will still complete.
   *
   * @param errors the errors to set
   */
  public void setErrors(List<String> errors) {
    this.errors = errors;
  }

}
