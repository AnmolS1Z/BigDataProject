package edu.uchicago.quake;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

@Service
public class HBaseService implements AutoCloseable {

    private final Configuration config;
    private final Connection connection;

    public HBaseService() throws Exception {
        this.config = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(config);
    }

    // --- Helper methods to safely decode numbers from HBase ---

    private long readLong(Result result, String family, String qualifier) {
        byte[] bytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (bytes == null) {
            return 0L;
        }
        if (bytes.length == Long.BYTES) {
            // classic binary long
            return Bytes.toLong(bytes);
        }
        // fall back: interpret as text
        String s = Bytes.toString(bytes).trim();
        if (s.isEmpty()) return 0L;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            // last resort: if it looks like a double, parse then cast
            try {
                return (long) Double.parseDouble(s);
            } catch (NumberFormatException ex) {
                return 0L;
            }
        }
    }

    private double readDouble(Result result, String family, String qualifier) {
        byte[] bytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (bytes == null) {
            return 0.0;
        }
        if (bytes.length == Double.BYTES) {
            // classic binary double
            return Bytes.toDouble(bytes);
        }
        // fall back: interpret as text
        String s = Bytes.toString(bytes).trim();
        if (s.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private int readInt(Result result, String family, String qualifier) {
        byte[] bytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (bytes == null) {
            return 0;
        }
        if (bytes.length == Integer.BYTES) {
            return Bytes.toInt(bytes);
        }
        String s = Bytes.toString(bytes).trim();
        if (s.isEmpty()) return 0;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            try {
                return (int) Double.parseDouble(s);
            } catch (NumberFormatException ex) {
                return 0;
            }
        }
    }

    // --- Main lookup method used by the controller ---

    public QuakeRecord getQuakeRecord(String state, int year, int month) throws Exception {
        String mm = String.format("%02d", month);
        String rowKey = state.toUpperCase() + "#" + year + "#" + mm;

        TableName tableName = TableName.valueOf("anmolsandhu_quake_state_month_hb");
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (result.isEmpty()) {
                return null;
            }

            long quakeCount = readLong(result, "s", "quake_count");
            double maxMag = readDouble(result, "s", "max_mag");
            int label = readInt(result, "s", "label_quake_ge_4");
            double predProb = readDouble(result, "s", "pred_prob_ge_4");

            QuakeRecord record = new QuakeRecord();
            record.setState(state.toUpperCase());
            record.setYear(year);
            record.setMonth(month);
            record.setQuakeCount(quakeCount);
            record.setMaxMag(maxMag);
            record.setLabelQuakeGe4(label);
            record.setPredProbGe4(predProb);
            return record;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
