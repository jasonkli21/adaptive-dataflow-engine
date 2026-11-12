package ade;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/** Session-scoped bounded ADE client. Close each FlightStream before closing the client. */
public final class GatewayClient implements AutoCloseable {
    private final RootAllocator allocator;
    private final FlightClient flight;
    private final FlightSqlClient sql;
    private final CallOption[] options;

    public GatewayClient(String host, int port, String token) throws Exception {
        if (!host.matches("([0-9]{1,3}\\.){3}[0-9]{1,3}|[0-9a-fA-F]*:[0-9a-fA-F:]+") || !InetAddress.getByName(host).isLoopbackAddress()) {
            throw new IllegalArgumentException("numeric loopback required");
        }
        var session = new byte[32];
        new SecureRandom().nextBytes(session);
        var headers = new FlightCallHeaders();
        headers.insert("authorization", "Bearer " + token);
        headers.insert("x-ade-session", HexFormat.of().formatHex(session));
        options = new CallOption[] {new HeaderCallOption(headers), CallOptions.timeout(11, TimeUnit.SECONDS)};
        allocator = new RootAllocator(32 * 1024 * 1024);
        try {
            flight = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build();
        } catch (RuntimeException e) {
            allocator.close();
            throw e;
        }
        sql = new FlightSqlClient(flight);
    }

    public FlightInfo sql(String statement) {
        return sql.execute(statement, options);
    }

    public Schema schema(String statement) {
        return sql.getExecuteSchema(statement, options).getSchema();
    }

    public FlightInfo metadata() {
        return sql.getSqlInfo(new int[] {0, 3, 10001}, options);
    }

    public FlightInfo plan(byte[] utf8Json) {
        var envelope = Any.newBuilder()
                .setTypeUrl("ade.gateway/QueryRequestV1")
                .setValue(ByteString.copyFrom(utf8Json))
                .build();
        return flight.getInfo(FlightDescriptor.command(envelope.toByteArray()), options);
    }

    public FlightStream read(FlightInfo info) {
        if (info.getEndpoints().size() != 1) {
            throw new IllegalStateException("expected exactly one Flight endpoint");
        }
        return flight.getStream(info.getEndpoints().get(0).getTicket(), options);
    }

    @Override
    public void close() throws Exception {
        try {
            sql.close();
        } finally {
            allocator.close();
        }
    }
}
