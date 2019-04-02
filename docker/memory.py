#!/usr/bin/env python
# coding=utf-8

# (C) Copyright 2017 Hewlett Packard Enterprise Development LP
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import sys

JVM_MAX_RATIO = os.environ.get('JVM_MAX_RATIO', '0.75')
JVM_MAX_MB = os.environ.get('JVM_MAX_MB', None)
MAX_OVERRIDE_MB = os.environ.get('MAX_OVERRIDE_MB', None)


def get_system_memory_mb():
    with open('/proc/meminfo', 'r') as f:
        for line in f.readlines():
            tokens = line.split()
            if tokens[0] != 'MemTotal:':
                continue

            assert tokens[2] == 'kB'
            total_kb = int(tokens[1])
            return total_kb / 1024

    return None


def get_cgroup_memory_mb():
    with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
        limit = int(f.read().strip())
        return limit / 1024 / 1024


def get_effective_memory_limit_mb():
    return min(get_system_memory_mb(), get_cgroup_memory_mb())


def main():
    if MAX_OVERRIDE_MB:
        print('{}m'.format(MAX_OVERRIDE_MB))
        return

    system_max = get_system_memory_mb()
    cgroup_max = get_cgroup_memory_mb()
    effective_max_ratio = float(JVM_MAX_RATIO)
    effective_max = int(min(system_max, cgroup_max) * effective_max_ratio)

    if JVM_MAX_MB:
        env_max = int(JVM_MAX_MB)
    else:
        env_max = effective_max

    if len(sys.argv) == 2:
        arg_max = int(sys.argv[1])
    else:
        arg_max = effective_max

    print('{:d}m'.format(min([
        effective_max,
        env_max,
        arg_max
    ])))


if __name__ == '__main__':
    main()
