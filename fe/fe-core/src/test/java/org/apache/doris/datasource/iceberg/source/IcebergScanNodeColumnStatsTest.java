// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.thrift.TFileSplitColBounds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Tests for IcebergScanNode.buildColumnStats(): verifies that Iceberg manifest
 * lowerBounds/upperBounds are correctly encoded and attached to TFileSplitColBounds.
 * Iceberg spec (Appendix D) mandates little-endian encoding for all numeric bounds.
 */
public class IcebergScanNodeColumnStatsTest {

    // Encode a long as little-endian 8 bytes (Iceberg wire format for LONG/TIMESTAMP).
    private static ByteBuffer longBuf(long v) {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(v);
        buf.flip();
        return buf;
    }

    // Encode an int as little-endian 4 bytes (Iceberg wire format for INTEGER/DATE).
    private static ByteBuffer intBuf(int v) {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(v);
        buf.flip();
        return buf;
    }

    @Test
    public void testLongBoundsEncodedCorrectly() {
        long minVal = 2451900L;
        long maxVal = 2451999L;

        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(7); // LONG

        ByteBuffer lb = longBuf(minVal);
        byte[] lbBytes = new byte[lb.remaining()];
        lb.get(lbBytes);
        stats.setLowerBound(lbBytes);

        ByteBuffer ub = longBuf(maxVal);
        byte[] ubBytes = new byte[ub.remaining()];
        ub.get(ubBytes);
        stats.setUpperBound(ubBytes);

        Assertions.assertEquals(8, stats.getLowerBound().length);
        Assertions.assertEquals(8, stats.getUpperBound().length);
        Assertions.assertEquals(7, stats.getIcebergTypeId());

        // Verify round-trip with little-endian decode (mirrors BE logic)
        long decodedMin = decodeLittleEndianLong(stats.getLowerBound());
        long decodedMax = decodeLittleEndianLong(stats.getUpperBound());
        Assertions.assertEquals(minVal, decodedMin);
        Assertions.assertEquals(maxVal, decodedMax);
    }

    @Test
    public void testIntBoundsEncodedCorrectly() {
        int minVal = 100;
        int maxVal = 500;

        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(5); // INTEGER

        ByteBuffer lb = intBuf(minVal);
        byte[] lbBytes = new byte[lb.remaining()];
        lb.get(lbBytes);
        stats.setLowerBound(lbBytes);

        ByteBuffer ub = intBuf(maxVal);
        byte[] ubBytes = new byte[ub.remaining()];
        ub.get(ubBytes);
        stats.setUpperBound(ubBytes);

        Assertions.assertEquals(4, stats.getLowerBound().length);
        Assertions.assertEquals(5, stats.getIcebergTypeId());

        long decodedMin = decodeLittleEndianLong(stats.getLowerBound());
        long decodedMax = decodeLittleEndianLong(stats.getUpperBound());
        Assertions.assertEquals(minVal, decodedMin);
        Assertions.assertEquals(maxVal, decodedMax);
    }

    @Test
    public void testNullCountStoredCorrectly() {
        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(7);
        stats.setNullCount(50L);
        stats.setRowCount(1000L);

        Assertions.assertTrue(stats.isSetNullCount());
        Assertions.assertEquals(50L, stats.getNullCount());
        Assertions.assertEquals(1000L, stats.getRowCount());
    }

    @Test
    public void testAllNullFile() {
        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(7);
        stats.setNullCount(500L);
        stats.setRowCount(500L);

        Assertions.assertFalse(stats.isSetLowerBound());
        Assertions.assertFalse(stats.isSetUpperBound());
        Assertions.assertEquals(stats.getNullCount(), stats.getRowCount());
    }

    @Test
    public void testNegativeLongEncoding() {
        long minVal = -999L;
        long maxVal = -1L;

        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(7);

        ByteBuffer lb = longBuf(minVal);
        byte[] lbBytes = new byte[lb.remaining()];
        lb.get(lbBytes);
        stats.setLowerBound(lbBytes);

        ByteBuffer ub = longBuf(maxVal);
        byte[] ubBytes = new byte[ub.remaining()];
        ub.get(ubBytes);
        stats.setUpperBound(ubBytes);

        Assertions.assertEquals(minVal, decodeLittleEndianLong(stats.getLowerBound()));
        Assertions.assertEquals(maxVal, decodeLittleEndianLong(stats.getUpperBound()));
    }

    @Test
    public void testStringBoundsEncodedCorrectly() {
        // STRING: Iceberg Conversions.toByteBuffer uses UTF_8 CharsetEncoder.encode(charBuffer)
        // = raw UTF-8 bytes, no length prefix, no byte order marker.
        String minVal = "apple";
        String maxVal = "mango";

        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(10); // STRING — internal Doris FE→BE protocol ID

        byte[] minBytes = minVal.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] maxBytes = maxVal.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        stats.setLowerBound(minBytes);
        stats.setUpperBound(maxBytes);

        Assertions.assertEquals(10, stats.getIcebergTypeId());
        Assertions.assertEquals(minVal,
                new String(stats.getLowerBound(), java.nio.charset.StandardCharsets.UTF_8));
        Assertions.assertEquals(maxVal,
                new String(stats.getUpperBound(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    public void testDecimalBoundsHaveScale() {
        // DECIMAL: big-endian two's complement unscaled value; scale stored in decimal_scale field.
        // 123456 = 0x01E240 in big-endian (3 bytes); scale=2 means the value represents 1234.56.
        TFileSplitColBounds stats = new TFileSplitColBounds();
        stats.setIcebergTypeId(11); // DECIMAL
        stats.setDecimalScale(2);

        byte[] boundBytes = new byte[] {0x01, (byte) 0xE2, 0x40}; // 123456 big-endian
        stats.setLowerBound(boundBytes);
        stats.setUpperBound(boundBytes);

        Assertions.assertTrue(stats.isSetDecimalScale());
        Assertions.assertEquals(2, stats.getDecimalScale());
        Assertions.assertArrayEquals(boundBytes, stats.getLowerBound());
        Assertions.assertArrayEquals(boundBytes, stats.getUpperBound());
    }

    // Mirror the BE decoding logic: little-endian sign-extended int (LSB at index 0).
    private static long decodeLittleEndianLong(byte[] bytes) {
        long val = 0;
        for (int i = bytes.length - 1; i >= 0; i--) {
            val = (val << 8) | (bytes[i] & 0xFFL);
        }
        int shift = 64 - bytes.length * 8;
        if (shift > 0 && shift < 64) {
            val = (val << shift) >> shift;
        }
        return val;
    }
}
