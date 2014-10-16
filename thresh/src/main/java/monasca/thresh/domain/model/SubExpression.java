package monasca.thresh.domain.model;

import java.io.Serializable;

import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.domain.common.AbstractEntity;

public class SubExpression extends AbstractEntity implements Serializable {
  private static final long serialVersionUID = -5485473840376995997L;

  private AlarmSubExpression alarmSubExpression;

  public SubExpression() {
  }

  public SubExpression(String id, AlarmSubExpression alarmSubExpression) {
    this.id = id;
    this.alarmSubExpression = alarmSubExpression;
  }

  public AlarmSubExpression getAlarmSubExpression() {
    return alarmSubExpression;
  }

  public void setAlarmSubExpression(AlarmSubExpression alarmSubExpression) {
    this.alarmSubExpression = alarmSubExpression;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((alarmSubExpression == null) ? 0 : alarmSubExpression.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SubExpression other = (SubExpression) obj;
    if (alarmSubExpression == null) {
      if (other.alarmSubExpression != null)
        return false;
    } else if (!alarmSubExpression.equals(other.alarmSubExpression))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format("SubExpression [id=%s. alarmSubExpression=%s]", id, alarmSubExpression);
  }
}
