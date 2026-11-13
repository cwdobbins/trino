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
package io.trino.parquet.reader.decoders;

import io.airlift.slice.Slice;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedByteDecoder;
import io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedShortDecoder;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestDeltaBinaryPackedNarrowIntegers
{
    @Test
    public void testByteDeltasLargerThanByteRange()
            throws IOException
    {
        for (int size : new int[] {2, 3, 10, 31, 32, 33, 127, 128, 129, 1000}) {
            for (int pattern = 0; pattern < 3; pattern++) {
                int[] values = values(size, Byte.MIN_VALUE, Byte.MAX_VALUE, pattern);
                byte[] expected = new byte[size];
                for (int index = 0; index < size; index++) {
                    expected[index] = (byte) values[index];
                }
                Slice encoded = encode(values);
                for (int batchSize : new int[] {1, 2, 7, 32, 127}) {
                    var decoder = new DeltaBinaryPackedByteDecoder();
                    decoder.init(new SimpleSliceInputStream(encoded));
                    byte[] actual = new byte[size + 2];
                    for (int offset = 0; offset < size; offset += batchSize) {
                        decoder.read(actual, offset + 1, min(batchSize, size - offset));
                    }
                    assertThat(Arrays.copyOfRange(actual, 1, size + 1)).containsExactly(expected);
                }
                for (int skip : new int[] {1, size / 2, size - 1}) {
                    var decoder = new DeltaBinaryPackedByteDecoder();
                    decoder.init(new SimpleSliceInputStream(encoded));
                    decoder.skip(skip);
                    byte[] actual = new byte[size - skip];
                    decoder.read(actual, 0, actual.length);
                    assertThat(actual).containsExactly(Arrays.copyOfRange(expected, skip, size));
                }
            }
        }
    }

    @Test
    public void testShortDeltasLargerThanShortRange()
            throws IOException
    {
        for (int size : new int[] {2, 3, 10, 31, 32, 33, 127, 128, 129, 1000}) {
            for (int pattern = 0; pattern < 3; pattern++) {
                int[] values = values(size, Short.MIN_VALUE, Short.MAX_VALUE, pattern);
                short[] expected = new short[size];
                for (int index = 0; index < size; index++) {
                    expected[index] = (short) values[index];
                }
                Slice encoded = encode(values);
                for (int batchSize : new int[] {1, 2, 7, 32, 127}) {
                    var decoder = new DeltaBinaryPackedShortDecoder();
                    decoder.init(new SimpleSliceInputStream(encoded));
                    short[] actual = new short[size + 2];
                    for (int offset = 0; offset < size; offset += batchSize) {
                        decoder.read(actual, offset + 1, min(batchSize, size - offset));
                    }
                    assertThat(Arrays.copyOfRange(actual, 1, size + 1)).containsExactly(expected);
                }
                for (int skip : new int[] {1, size / 2, size - 1}) {
                    var decoder = new DeltaBinaryPackedShortDecoder();
                    decoder.init(new SimpleSliceInputStream(encoded));
                    decoder.skip(skip);
                    short[] actual = new short[size - skip];
                    decoder.read(actual, 0, actual.length);
                    assertThat(actual).containsExactly(Arrays.copyOfRange(expected, skip, size));
                }
            }
        }
    }

    private static int[] values(int size, int minimum, int maximum, int pattern)
    {
        int[] values = new int[size];
        Random random = new Random(1234);
        for (int index = 0; index < size; index++) {
            values[index] = switch (pattern) {
                case 0 -> index % 2 == 0 ? minimum : maximum;
                case 1 -> index % 2 == 0 ? maximum : minimum;
                case 2 -> random.nextInt(maximum - minimum + 1) + minimum;
                default -> throw new IllegalArgumentException("Unknown pattern: " + pattern);
            };
        }
        return values;
    }

    private static Slice encode(int[] values)
            throws IOException
    {
        try (var writer = new DeltaBinaryPackingValuesWriterForInteger(128, 1024, HeapByteBufferAllocator.getInstance())) {
            for (int value : values) {
                writer.writeInteger(value);
            }
            return wrappedBuffer(writer.getBytes().toByteArray());
        }
    }
}
