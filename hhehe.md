# LSEG Daily Processing — End-to-End Performance Fix

## Problem

`processLsegFromDatabase` OOMs and spikes CPU at 20M `lseg_quotes` records because:

- The whole day's loop runs inside one `@Transactional`, and history-entity lookups per
  batch pile up in the Hibernate session for the entire run.
- `allChangedQuotes` / `allQuoteIdToHashMap` accumulate every changed record for the
  *entire day* before anything is written out.
- The whole CSV is built as a single in-memory `String` before upload.
- `findByLastModifiedDatetimeBetweenOptimized` uses `Page<>` (a `COUNT(*)` query runs on
  every page) with `OFFSET` pagination and no `ORDER BY` (O(n²) cost + non-deterministic
  row order across pages).
- History is written back with **one `upsertOne` DB call per changed record** — on day
  one, with no existing history, that's ~20M individual round trips in a single
  transaction.

## Fix summary

1. Keyset (seek) pagination instead of `OFFSET`, no `COUNT(*)` per page.
2. Stream the CSV to a temp file per batch instead of holding it all in memory.
3. Stage pending `(quote_id, hash)` pairs into a small table as batches are processed,
   then commit them to `lseg_quotes_fdm_history` with **one set-based bulk upsert**
   after the file upload succeeds (or discard them on failure).
4. No single long-lived transaction — each DB operation gets its own short transaction.

`quote_id` is already unique, so the staging table doesn't need a `correlation_id` —
it's keyed on `quote_id` alone. This assumes only one run of this job processes a given
date range at a time (no overlapping concurrent runs writing to staging
simultaneously); if that assumption ever breaks, a run-scoping column would need to come
back.

---

## 1. SQL migration

```sql
-- Keyset pagination needs a covering, ordered index
CREATE INDEX idx_lseg_quotes_lastmod_id ON lseg_quotes (last_modified_datetime, id);

-- Required for ON DUPLICATE KEY UPDATE to target the correct row
ALTER TABLE lseg_quotes_fdm_history ADD UNIQUE KEY uq_lqfh_quote_id (quote_id);

-- Staging table: holds pending (quote_id, hash) pairs until the file upload succeeds
CREATE TABLE lseg_quotes_fdm_history_staging (
    quote_id   VARCHAR(64) NOT NULL,
    sent_hash  BINARY(16)  NOT NULL,
    created_at DATETIME    NOT NULL,
    PRIMARY KEY (quote_id)
) ENGINE=InnoDB;
```

---

## 2. `LsegQuoteRepository.java` — replace the OFFSET query

```java
@Query(
        value = """
            SELECT q.id, q.quote_id, q.ric, q.sedol, q.ticker, q.exchange_code
            FROM lseg_quotes q
            WHERE q.last_modified_datetime BETWEEN :startOfDay AND :endOfDay
              AND q.id > :lastId
            ORDER BY q.id ASC
            LIMIT :size
            """,
        nativeQuery = true)
List<LsegQuoteProjection> findNextBatchByLastModifiedDatetimeBetween(
        @Param("startOfDay") OffsetDateTime startOfDay,
        @Param("endOfDay") OffsetDateTime endOfDay,
        @Param("lastId") long lastId,
        @Param("size") int size);
```

Plain `List<>` — no `Page`/`Slice` needed. The loop is driven by `lastId` and stops when
a batch comes back smaller than `size`. Delete `findByLastModifiedDatetimeBetweenOptimized`.

---

## 3. New: `LsegQuoteFdmHistoryStagingRepository.java`

```java
// src/main/java/com/nagacoder/test/eis_modulith/staticData/internal/lsegv3/repository/LsegQuoteFdmHistoryStagingRepository.java
package com.nagacoder.test.sakuragon.staticData.internal.lsegv3.repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class LsegQuoteFdmHistoryStagingRepository {

    private static final int JDBC_BATCH_SIZE = 1000;

    private final JdbcTemplate jdbcTemplate;

    public LsegQuoteFdmHistoryStagingRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void stageBatch(Map<String, byte[]> quoteIdToHashMap) {
        if (quoteIdToHashMap.isEmpty()) {
            return;
        }

        List<String> quoteIds = List.copyOf(quoteIdToHashMap.keySet());
        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        String sql = "INSERT INTO lseg_quotes_fdm_history_staging (quote_id, sent_hash, created_at) "
                + "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE sent_hash = VALUES(sent_hash), created_at = VALUES(created_at)";

        for (int start = 0; start < quoteIds.size(); start += JDBC_BATCH_SIZE) {
            List<String> chunk = quoteIds.subList(start, Math.min(start + JDBC_BATCH_SIZE, quoteIds.size()));

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    String quoteId = chunk.get(i);
                    ps.setString(1, quoteId);
                    ps.setBytes(2, quoteIdToHashMap.get(quoteId));
                    ps.setTimestamp(3, now);
                }

                @Override
                public int getBatchSize() {
                    return chunk.size();
                }
            });
        }
    }

    @Transactional
    public int commitStagedHistory() {
        int updated = jdbcTemplate.update(
                "INSERT INTO lseg_quotes_fdm_history (quote_id, sent_hash, last_sent) "
                        + "SELECT quote_id, sent_hash, NOW() FROM lseg_quotes_fdm_history_staging "
                        + "ON DUPLICATE KEY UPDATE sent_hash = VALUES(sent_hash), last_sent = VALUES(last_sent)");
        discardStagedHistory();
        return updated;
    }

    @Transactional
    public void discardStagedHistory() {
        jdbcTemplate.update("TRUNCATE TABLE lseg_quotes_fdm_history_staging");
    }
}
```

---

## 4. `LsegChangeDetectionService.java` — full replacement

```java
// src/main/java/com/nagacoder/test/eis_modulith/staticData/internal/lsegv3/service/LsegChangeDetectionService.java
package com.nagacoder.test.sakuragon.staticData.internal.lsegv3.service;

import com.nagacoder.test.sakuragon.staticData.internal.lsegv3.domain.LsegQuote;
import com.nagacoder.test.sakuragon.staticData.internal.lsegv3.domain.LsegQuoteFdmHistory;
import com.nagacoder.test.sakuragon.staticData.internal.lsegv3.repository.LsegQuoteFdmHistoryRepository;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class LsegChangeDetectionService {

    private static final ThreadLocal<MessageDigest> MD5_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    });

    private final LsegQuoteFdmHistoryRepository historyRepository;

    public LsegChangeDetectionService(LsegQuoteFdmHistoryRepository historyRepository) {
        this.historyRepository = historyRepository;
    }

    public byte[] computeHash(LsegQuote quote) {
        return md5Hash(computeHashInput(quote.getRic(), quote.getSedol(), quote.getTicker()));
    }

    private String computeHashInput(String ric, String sedol, String ticker) {
        List<String> parts = new ArrayList<>(3);
        if (ric != null) parts.add(ric);
        if (sedol != null) parts.add(sedol);
        if (ticker != null) parts.add(ticker);
        return String.join("|", parts);
    }

    private byte[] md5Hash(String input) {
        MessageDigest digest = MD5_DIGEST.get();
        digest.reset();
        return digest.digest(input.getBytes());
    }

    public record ChangeResult(List<LsegQuote> changedQuotes, Map<String, byte[]> hashesByQuoteId) {}

    public ChangeResult filterChangedQuotes(List<LsegQuote> quotes) {
        if (quotes.isEmpty()) {
            return new ChangeResult(List.of(), Map.of());
        }

        List<String> quoteIds = new ArrayList<>(quotes.size());
        Map<String, byte[]> currentHashes = new HashMap<>(quotes.size() * 2);

        for (LsegQuote quote : quotes) {
            quoteIds.add(quote.getQuoteId());
            currentHashes.put(quote.getQuoteId(), computeHash(quote));
        }

        List<LsegQuoteFdmHistory> histories = historyRepository.findByQuoteIdIn(quoteIds);
        Set<String> unchangedIds = new HashSet<>();

        for (LsegQuoteFdmHistory history : histories) {
            byte[] currentHash = currentHashes.get(history.getQuoteId());
            if (Arrays.equals(history.getSentHash(), currentHash)) {
                unchangedIds.add(history.getQuoteId());
            }
        }

        log.info("Detected {} unchanged records out of {}", unchangedIds.size(), quotes.size());

        List<LsegQuote> changedQuotes =
                quotes.stream().filter(q -> !unchangedIds.contains(q.getQuoteId())).toList();

        Map<String, byte[]> changedHashes = new HashMap<>(changedQuotes.size() * 2);
        for (LsegQuote quote : changedQuotes) {
            changedHashes.put(quote.getQuoteId(), currentHashes.get(quote.getQuoteId()));
        }

        return new ChangeResult(changedQuotes, changedHashes);
    }
}
```

Removed vs. original: the `batchSize`/`@Value` field and `recordSentBatch` (the per-row
upsert loop) — superseded by `LsegQuoteFdmHistoryStagingRepository`.

---

## 5. `DailyProcessingService.java` — replace `processLsegFromDatabase`

Add a field + constructor injection first:

```java
private final LsegQuoteFdmHistoryStagingRepository stagingRepository;
```

```java
private String stripHeaderLine(String csv) {
    int idx = csv.indexOf('\n');
    return idx == -1 ? "" : csv.substring(idx + 1);
}

public ProcessingResult processLsegFromDatabase(
        String correlationId, String dateStr, LocalDateTime startTime) {

    log.info("Processing LSEG data from database for date: {}", dateStr);

    try {
        Optional<OffsetDateTime[]> dateRange = dateFileFilter.toOffsetDateTimeRange(dateStr);
        if (dateRange.isEmpty()) {
            return ProcessingResult.error(
                    correlationId, SftpFileType.LSEG, dateStr,
                    "Failed to convert date to OffsetDateTime range", startTime, LocalDateTime.now());
        }

        OffsetDateTime startOfDay = dateRange.get()[0];
        OffsetDateTime endOfDay = dateRange.get()[1];

        log.info("Starting batch processing with size: {}", batchProcessSize);

        Path tempFile = Files.createTempFile("lseg-staticdata-", ".csv");
        long lastId = 0L;
        int batchNumber = 0;
        int totalChanged = 0;
        boolean anyChanges = false;

        try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
            while (true) {
                // TODO: add findNextBatch(startOfDay, endOfDay, lastId, size) to lsegV3DataService,
                // delegating to LsegQuoteRepository.findNextBatchByLastModifiedDatetimeBetween(...)
                List<LsegQuoteProjection> projections =
                        lsegV3DataService.findNextBatch(startOfDay, endOfDay, lastId, batchProcessSize);

                if (projections.isEmpty()) {
                    break;
                }

                batchNumber++;
                List<LsegQuote> batchQuotes = projections.stream().map(this::projectionToQuote).toList();
                log.info("Processing batch {}: {} records", batchNumber, batchQuotes.size());

                LsegChangeDetectionService.ChangeResult changeResult =
                        changeDetectionService.filterChangedQuotes(batchQuotes);

                if (!changeResult.changedQuotes().isEmpty()) {
                    log.info("Detected {} changed records in batch {} out of {}",
                            changeResult.changedQuotes().size(), batchNumber, batchQuotes.size());

                    List<LsegStaticDataRecord> batchRecords =
                            changeResult.changedQuotes().stream().map(this::convertToLsegRecord).toList();

                    // Assumes generateCsv(records) returns header + rows each call — strip the
                    // header for every batch after the first so the file has one header line.
                    String csvChunk = lsegOutputGenerator.generateCsv(batchRecords);
                    writer.write(anyChanges ? stripHeaderLine(csvChunk) : csvChunk);
                    writer.newLine();

                    stagingRepository.stageBatch(changeResult.hashesByQuoteId());

                    anyChanges = true;
                    totalChanged += changeResult.changedQuotes().size();
                } else {
                    log.info("No changes detected in batch {}", batchNumber);
                }

                lastId = projections.get(projections.size() - 1).getId();
                if (projections.size() < batchProcessSize) {
                    break;
                }
            }
        }

        if (!anyChanges) {
            Files.deleteIfExists(tempFile);
            log.info("No values changed for any records on date {}", dateStr);
            return ProcessingResult.success(
                    correlationId, SftpFileType.LSEG, dateStr, List.of(), startTime, LocalDateTime.now());
        }

        log.info("Total changed records across all batches: {}", totalChanged);

        String fileName = lsegOutputGenerator.generateFileName();

        // TODO: add an overload to StaticDataFileUploadService that accepts a local Path and
        // streams it to disk/S3, instead of requiring the whole CSV as a String.
        StaticDataFileUploadService.UploadResult uploadResult =
                fileUploadService.saveFile(fileName, tempFile, FileCategory.STATICDATA);

        log.info("File uploaded successfully: local={}, s3={}",
                uploadResult.localPath(), uploadResult.s3Path().orElse("disabled"));

        com.nagacoder.test.sakuragon.staticData.internal.sftp.FileUploadService.UploadResult rsftResult =
                rsftUploadService.uploadWithGeographicRouting(List.of(uploadResult.localPath()), SftpFileType.LSEG);

        if (rsftResult.isSuccess()) {
            log.info("File uploaded to RSFT servers: {}", fileName);
            int committed = stagingRepository.commitStagedHistory();
            log.info("Committed {} records to history", committed);
        } else {
            log.warn("RSFT upload had failures: {} successful, {} failed",
                    rsftResult.successCount(), rsftResult.failureCount());
            log.info("Skipping history update due to RSFT upload failure");
            stagingRepository.discardStagedHistory();
        }

        Files.deleteIfExists(tempFile);

        return ProcessingResult.success(
                correlationId, SftpFileType.LSEG, dateStr, List.of(fileName), startTime, LocalDateTime.now());

    } catch (Exception e) {
        log.error("Error processing LSEG data from database for date: {}", dateStr, e);
        return ProcessingResult.error(
                correlationId, SftpFileType.LSEG, dateStr, e.getMessage(), startTime, LocalDateTime.now());
    }
}
```

**Remove the `@Transactional` annotation from this method** — nothing needs one long
transaction anymore; `stageBatch`, `commitStagedHistory`, and `discardStagedHistory`
are each transactional on their own.

---

## Remaining integration TODOs (files not shared, so left as stubs)

1. `lsegV3DataService.findNextBatch(...)` — delegates to
   `LsegQuoteRepository.findNextBatchByLastModifiedDatetimeBetween(...)`.
2. `StaticDataFileUploadService.saveFile(fileName, Path, category)` — stream from disk
   instead of taking a `String`.
3. Confirm whether `generateCsv(records)` emits a header on every call — if not, drop
   the `stripHeaderLine` call.

## Net effect on a 20M-record / day-one run

- Memory: bounded by one batch's worth of records at a time — no 20M-element lists,
  maps, or a multi-GB CSV `String`.
- CPU/DB load: no `COUNT(*)` per page, no `OFFSET` scan degradation, no 20M individual
  upserts — replaced by batched staging inserts + one bulk `INSERT ... ON DUPLICATE KEY
  UPDATE` at the end.
- Correctness: deterministic keyset order removes the skip/duplicate risk of the
  previous unordered `OFFSET` query.
