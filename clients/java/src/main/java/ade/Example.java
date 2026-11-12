package ade;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.Schema;

/** Runs an ADE request and emits exact cross-language test values. */
public final class Example {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static List<Map<String, Object>> fields(Schema schema) {
        return schema.getFields().stream().map(field -> Map.<String, Object>of(
                "name", field.getName(), "type", field.getType().toString(), "nullable", field.isNullable()
        )).toList();
    }

    private static String value(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof UInt8Vector u64) {
            return Long.toUnsignedString(u64.get(row));
        }
        if (vector instanceof UInt4Vector u32) {
            return Integer.toUnsignedString(u32.get(row));
        }
        if (vector instanceof TimeStampMilliVector timestamp) {
            return Long.toString(timestamp.get(row));
        }
        if (vector instanceof Float8Vector f64) {
            return Long.toUnsignedString(Double.doubleToRawLongBits(f64.get(row)), 16);
        }
        return vector.getObject(row).toString();
    }

    private static Map<String, Object> result(GatewayClient client, FlightInfo info) throws Exception {
        var rows = new ArrayList<List<String>>();
        try (var stream = client.read(info)) {
            var root = stream.getRoot();
            if (!root.getSchema().equals(info.getSchema())) {
                throw new IllegalStateException("Flight schema mismatch");
            }
            while (stream.next()) {
                for (int index = 0; index < root.getRowCount(); index++) {
                    var row = new ArrayList<String>();
                    for (var vector : root.getFieldVectors()) {
                        row.add(value(vector, index));
                    }
                    rows.add(row);
                }
            }
        }
        if (rows.size() != info.getRecords()) {
            throw new IllegalStateException("Flight row count mismatch");
        }
        return Map.of("schema", fields(info.getSchema()), "rows", rows,
                "receipt", JSON.readTree(info.getAppMetadata()));
    }

    public static void main(String[] args) throws Exception {
        try (var client = new GatewayClient(args[0], Integer.parseInt(args[1]), System.getenv("ADE_GATEWAY_TOKEN"))) {
            Object response = switch (args[2]) {
                case "sql" -> result(client, client.sql(args[3]));
                case "plan" -> result(client, client.plan(Files.readAllBytes(Path.of(args[3]))));
                case "repeat_sql" -> {
                    var samples = new ArrayList<Map<String, Object>>();
                    for (int index = 0; index < 12; index++) {
                        long start = System.nanoTime();
                        var observed = result(client, client.sql(args[3]));
                        samples.add(Map.of("wall_ns", System.nanoTime() - start, "observed", observed));
                    }
                    yield samples;
                }
                case "metadata" -> result(client, client.metadata());
                case "schema" -> Map.of("schema", fields(client.schema(args[3])));
                default -> throw new IllegalArgumentException("expected sql, plan, schema, or metadata");
            };
            System.out.println(JSON.writeValueAsString(response));
        } catch (FlightRuntimeException e) {
            System.out.println(JSON.writeValueAsString(Map.of("code", e.status().code().name())));
            System.exit(2);
        }
    }
}
