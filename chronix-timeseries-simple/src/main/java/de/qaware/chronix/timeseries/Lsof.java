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
 * An entry of the lsof
 */
public class Lsof {

    private String command;
    private int pid;
    private String user;
    private String fd;
    private String type;
    private String device;
    private String size;
    private String node;
    private String name;

    private Lsof() {
        //avoid instances
    }

    public String getCommand() {
        return command;
    }

    public int getPid() {
        return pid;
    }

    public String getUser() {
        return user;
    }

    public String getFd() {
        return fd;
    }

    public String getType() {
        return type;
    }

    public String getDevice() {
        return device;
    }

    public String getSize() {
        return size;
    }

    public String getNode() {
        return node;
    }

    public String getName() {
        return name;
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
        Lsof rhs = (Lsof) obj;
        return new EqualsBuilder()
                .append(this.command, rhs.command)
                .append(this.pid, rhs.pid)
                .append(this.user, rhs.user)
                .append(this.fd, rhs.fd)
                .append(this.type, rhs.type)
                .append(this.device, rhs.device)
                .append(this.size, rhs.size)
                .append(this.node, rhs.node)
                .append(this.name, rhs.name)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(command)
                .append(pid)
                .append(user)
                .append(fd)
                .append(type)
                .append(device)
                .append(size)
                .append(node)
                .append(name)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("command", command)
                .append("pid", pid)
                .append("user", user)
                .append("fd", fd)
                .append("type", type)
                .append("device", device)
                .append("size", size)
                .append("node", node)
                .append("name", name)
                .toString();
    }


    public static final class Builder {

        /**
         * The time series object
         */
        private Lsof lsof;

        public Builder() {
            lsof = new Lsof();
        }

        public Builder command(String command) {
            lsof.command = command;
            return this;
        }

        public Builder pid(int pid) {
            lsof.pid = pid;
            return this;
        }

        public Builder user(String user) {
            lsof.user = user;
            return this;
        }

        public Builder fd(String fd) {
            lsof.fd = fd;
            return this;
        }

        public Builder type(String type) {
            lsof.type = type;
            return this;
        }

        public Builder device(String device) {
            lsof.device = device;
            return this;
        }

        public Builder size(String size) {
            lsof.size = size;
            return this;
        }

        public Builder node(String node) {
            lsof.node = node;
            return this;
        }

        public Builder name(String name) {
            lsof.name = name;
            return this;
        }


        public Lsof build() {
            return lsof;
        }
    }

}
