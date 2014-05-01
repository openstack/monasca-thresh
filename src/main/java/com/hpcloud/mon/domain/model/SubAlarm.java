/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hpcloud.mon.domain.model;

import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.domain.common.AbstractEntity;

import java.io.Serializable;

/**
 * Sub-alarm. Decorates an AlarmSubExpression.
 */
public class SubAlarm extends AbstractEntity implements Serializable {
    private static final long serialVersionUID = -3946708553723868124L;

    private String alarmId;
    private AlarmSubExpression expression;
    private AlarmState state;
    private boolean noState;
    /**
     * Whether metrics for this sub-alarm are received sporadically.
     */
    private boolean sporadicMetric;

    public SubAlarm(String id, String alarmId, AlarmSubExpression expression) {
        this(id, alarmId, expression, AlarmState.UNDETERMINED);
    }

    // Need this for kryo serialization/deserialization.  Fixes a bug in default java
    // serialization/deserialization where id was not being set.  See resources/storm.yaml
    // file for how to handle serialization/deserialization with kryo.
    public SubAlarm() {
    }

    public SubAlarm(String id, String alarmId, AlarmSubExpression expression, AlarmState state) {
        this.id = id;
        this.alarmId = alarmId;
        this.expression = expression;
        this.state = state;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubAlarm other = (SubAlarm) obj;
        if (alarmId == null) {
            if (other.alarmId != null)
                return false;
        } else if (!alarmId.equals(other.alarmId))
            return false;
        if (expression == null) {
            if (other.expression != null)
                return false;
        } else if (!expression.equals(other.expression))
            return false;
        if (state != other.state)
            return false;
        return true;
    }

    public String getAlarmId() {
        return alarmId;
    }

    public AlarmSubExpression getExpression() {
        return expression;
    }

    public AlarmState getState() {
        return state;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((alarmId == null) ? 0 : alarmId.hashCode());
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        return result;
    }

    public boolean isSporadicMetric() {
        return sporadicMetric;
    }

    public void setSporadicMetric(boolean sporadicMetric) {
        this.sporadicMetric = sporadicMetric;
    }

    public void setState(AlarmState state) {
        this.state = state;
    }

    public boolean isNoState() {
        return noState;
    }

    public void setNoState(boolean noState) {
        this.noState = noState;
    }

    @Override
    public String toString() {
        return String.format("SubAlarm [id=%s, alarmId=%s, expression=%s, state=%s noState=%s]", id, alarmId,
                expression, state, noState);
    }

    /**
     * Determine if this SubAlarm and 'other' could reuse saved measurements. Only possible
     * only operator and/or threshold are the only properties from the expression that are different
     * @param other SubAlarm to compare to
     * @return true if 'other' is "compatible", false otherwise
     */
    public boolean isCompatible(final SubAlarm other) {
        if (!this.expression.getMetricDefinition().equals(other.expression.getMetricDefinition()))
            return false;
        if (!this.expression.getFunction().equals(other.expression.getFunction()))
            return false;
        if (this.expression.getPeriod() != other.expression.getPeriod())
            return false;
        if (this.expression.getPeriods() != other.expression.getPeriods())
            return false;
        // Operator and Threshold can vary
        return true;
    }
}
