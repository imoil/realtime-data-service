package com.telemetry.gateway;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

@Service
public class ArrowStreamWriterService {

    public static final Schema ARROW_SCHEMA = new Schema(List.of(
            new Field("sensor_id", FieldType.nullable(new ArrowType.Int(32, false)), null),
            new Field("ts_ms", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("val", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
    ));

    public void writeDataPoints(List<Am4DataPoint> points, OutputStream outputStream) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))) {

            writer.start();

            UInt4Vector sensorIdVector = (UInt4Vector) root.getVector("sensor_id");
            Float8Vector tsMsVector = (Float8Vector) root.getVector("ts_ms");
            Float8Vector valVector = (Float8Vector) root.getVector("val");

            int rowCount = points.size();
            sensorIdVector.allocateNew(rowCount);
            tsMsVector.allocateNew(rowCount);
            valVector.allocateNew(rowCount);

            for (int i = 0; i < rowCount; i++) {
                Am4DataPoint point = points.get(i);
                sensorIdVector.set(i, point.sensorId());
                tsMsVector.set(i, point.tsMs());
                valVector.set(i, point.val());
            }

            sensorIdVector.setValueCount(rowCount);
            tsMsVector.setValueCount(rowCount);
            valVector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            writer.writeBatch();
            writer.end();
        }
    }
}
