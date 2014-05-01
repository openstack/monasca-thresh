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
package com.hpcloud.mon.infrastructure.thresholding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyFinder {
    private static final Logger LOG = LoggerFactory.getLogger(PropertyFinder.class);

    private PropertyFinder()
    {
    }

    public static int getIntProperty(final String name,
                                     final int defaultValue,
                                     final int minValue,
                                     final int maxValue) {
        final String valueString = System.getProperty(name);
        if ((valueString != null) && !valueString.isEmpty()) {
            try {
                final int newValue = Integer.parseInt(valueString);
                if ((newValue >= minValue) && (newValue <= maxValue)) {
                    return newValue;
                }
                LOG.warn("Invalid value {} for property '{}' must be >= {} and <= {}, using default value of {}",
                        valueString, name, minValue, maxValue, defaultValue);
            }
            catch (NumberFormatException nfe) {
                LOG.warn("Not an integer value '{}' for property '{}', using default value of {}", valueString,
                        name, defaultValue);
            }
        }
        return defaultValue;
    }
}
