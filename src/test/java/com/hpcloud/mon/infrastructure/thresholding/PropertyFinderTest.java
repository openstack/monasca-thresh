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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class PropertyFinderTest {

    private static String PROPERTY_NAME = "com.hpcloud.mon.infrastructure.thresholding.Prop";

    @BeforeMethod
    public void beforeMethod() {
        System.clearProperty(PROPERTY_NAME);
    }

    public void shouldUseNewValue() {
        final int expectedValue = 45;
        System.setProperty(PROPERTY_NAME, String.valueOf(expectedValue));
        assertEquals(expectedValue, PropertyFinder.getIntProperty(PROPERTY_NAME, 30, 0, Integer.MAX_VALUE));
    }

    public void shouldUseDefaultValueBecausePropertyNotSet() {
        final int defaultValue = 45;
        assertEquals(defaultValue, PropertyFinder.getIntProperty(PROPERTY_NAME, defaultValue, 0, Integer.MAX_VALUE));
    }

    public void shouldUseDefaultValueBecausePropertyNotANumber() {
        final int defaultValue = 45;
        System.setProperty(PROPERTY_NAME, "AAA");
        assertEquals(defaultValue, PropertyFinder.getIntProperty(PROPERTY_NAME, defaultValue, 0, Integer.MAX_VALUE));
    }

    public void shouldUseDefaultValueBecausePropertyTooSmall() {
        final int defaultValue = 45;
        System.setProperty(PROPERTY_NAME, "0");
        assertEquals(defaultValue, PropertyFinder.getIntProperty(PROPERTY_NAME, defaultValue, 1, Integer.MAX_VALUE));
    }

    public void shouldUseDefaultValueBecausePropertyTooLarge() {
        final int defaultValue = 45;
        System.setProperty(PROPERTY_NAME, "10");
        assertEquals(defaultValue, PropertyFinder.getIntProperty(PROPERTY_NAME, defaultValue, 9, 9));
    }
}
