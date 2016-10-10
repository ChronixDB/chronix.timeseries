/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.timeseries;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by f.lautenschlager on 17.09.2016.
 */
public class Strace {

    private int pid;
    private String call;

    public Strace(int pid, String call) {
        this.pid = pid;
        this.call = call;
    }

    public int getPid() {
        return pid;
    }

    public String getCall() {
        return call;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("pid", pid)
                .append("call", call)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Strace rhs = (Strace) obj;
        return new EqualsBuilder()
                .append(this.pid, rhs.pid)
                .append(this.call, rhs.call)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(pid)
                .append(call)
                .toHashCode();
    }
}
