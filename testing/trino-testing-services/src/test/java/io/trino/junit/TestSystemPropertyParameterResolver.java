/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.junit;

import io.trino.junit.SystemPropertyParameterResolver.SystemProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SystemPropertyParameterResolver.class)
public class TestSystemPropertyParameterResolver
{
    private static final String PROPERTY_NAME = "somePropertyNameToPass";
    private static final String PROPERTY_VALUE = "secretValue";

    @BeforeAll
    public static void init()
    {
        System.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
    }

    @Test
    public void testPropertySet(@SystemProperty(PROPERTY_NAME) String property)
    {
        assertThat(property).isEqualTo(PROPERTY_VALUE);
    }
}
