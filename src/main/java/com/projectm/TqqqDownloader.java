package com.projectm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Downloads the last 10 years of TQQQ daily open/close prices from the
 * Yahoo Finance chart API and stores them in a SQLite table.
 *
 * Usage: java -cp ... com.projectm.TqqqDownloader [dbFile]
 * Default dbFile: tqqq.db
 */
public final class TqqqDownloader {

    private static final Logger LOG = Logger.getLogger(TqqqDownloader.class.getName());

    private static final String SYMBOL = "TQQQ";
    private static final int YEARS = 10;
    private static final String URL_TEMPLATE =
            "https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d&events=history";

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS tqqq_daily ("
                    + "trade_date TEXT PRIMARY KEY, "   // ISO yyyy-MM-dd
                    + "open_price REAL NOT NULL, "
                    + "close_price REAL NOT NULL)";

    private static final String UPSERT_SQL =
            "INSERT INTO tqqq_daily (trade_date, open_price, close_price) VALUES (?, ?, ?) "
                    + "ON CONFLICT(trade_date) DO UPDATE SET "
                    + "open_price = excluded.open_price, close_price = excluded.close_price";

    private TqqqDownloader() {
    }

    public static void main(String[] args) {
        String dbFile = args.length > 0 ? args[0] : "tqqq.db";
        try {
            LocalDate end = LocalDate.now(ZoneOffset.UTC);
            LocalDate start = end.minusYears(YEARS);

            JsonNode result = download(start, end);
            int stored = store(dbFile, result);
            LOG.info("Stored " + stored + " rows for " + SYMBOL + " from " + start + " to " + end
                    + " in " + dbFile);
        } catch (IOException | SQLException e) {
            LOG.log(Level.SEVERE, "Failed to download or store " + SYMBOL + " data", e);
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.log(Level.SEVERE, "Interrupted", e);
            System.exit(1);
        }
    }

    private static JsonNode download(LocalDate start, LocalDate end) throws IOException, InterruptedException {
        long period1 = start.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        long period2 = end.plusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        String url = String.format(URL_TEMPLATE, SYMBOL, period1, period2);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("User-Agent", "Mozilla/5.0")
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Unexpected HTTP status " + response.statusCode());
        }

        JsonNode result = new ObjectMapper().readTree(response.body()).path("chart").path("result").path(0);
        if (result.isMissingNode() || result.path("timestamp").isMissingNode()) {
            throw new IOException("Response contains no price data");
        }
        return result;
    }

    private static int store(String dbFile, JsonNode result) throws SQLException {
        JsonNode timestamps = result.path("timestamp");
        JsonNode quote = result.path("indicators").path("quote").path(0);
        JsonNode opens = quote.path("open");
        JsonNode closes = quote.path("close");
        ZoneId exchangeZone = ZoneId.of(result.path("meta").path("exchangeTimezoneName").asText("America/New_York"));

        int count = 0;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile)) {
            try (Statement st = conn.createStatement()) {
                st.execute(CREATE_SQL);
            }
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
                for (int i = 0; i < timestamps.size(); i++) {
                    JsonNode open = opens.path(i);
                    JsonNode close = closes.path(i);
                    if (open.isNull() || open.isMissingNode() || close.isNull() || close.isMissingNode()) {
                        continue; // skip days with incomplete data
                    }
                    LocalDate date = Instant.ofEpochSecond(timestamps.get(i).asLong())
                            .atZone(exchangeZone).toLocalDate();
                    ps.setString(1, date.toString());
                    ps.setDouble(2, round4(open.asDouble()));
                    ps.setDouble(3, round4(close.asDouble()));
                    ps.addBatch();
                    count++;
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
        return count;
    }

    private static double round4(double value) {
        return Math.round(value * 10_000d) / 10_000d;
    }
}
