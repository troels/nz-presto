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
package com.facebook.presto.spi.type;

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestChars
{
    @Test
    public void testTruncateToLengthAndTrimSpaces()
    {
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 1));
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("a  "), 1));
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("abc"), 1));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 0));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 3));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 4));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 5));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice("  "), 1));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice(""), 1));
    }
}
