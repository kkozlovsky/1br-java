package org.example;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

// 5611ms
// Abha=-32,4/18,0/69,1
public class SwarSolution {

    static class MutableResult {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        long count = 0;

        void update(final double temperature) {
            if (temperature < min) {
                min = temperature;
            }
            if (temperature > max) {
                max = temperature;
            }
            sum += temperature;
            count++;
        }

        void merge(final MutableResult other) {
            if (other.min < this.min) {
                this.min = other.min;
            }
            if (other.max > this.max) {
                this.max = other.max;
            }
            this.sum += other.sum;
            this.count += other.count;
        }

        double mean() {
            return sum / count;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min, mean(), max);
        }
    }

    static void main(final String[] args) throws Exception {
        final long start = System.currentTimeMillis();

        final Path filePath = Path.of("./measurements.txt");
        final RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r");
        final FileChannel channel = file.getChannel();
        final long fileSize = channel.size();

        // Определяем количество потоков
        final int numThreads = Runtime.getRuntime().availableProcessors();
        System.out.println("Количество потоков: " + numThreads);
        final long chunkSize = fileSize / numThreads;
        System.out.println("chunkSize: " + (chunkSize / (1024 * 1024)) + "MB");
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Map<String, MutableResult>>> futures = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            final long startPos = i * chunkSize;
            final long endPos = (i == numThreads - 1) ? fileSize : (i + 1) * chunkSize;

            futures.add(executor.submit(() -> processChunk(channel, startPos, endPos)));
        }

        final Map<String, MutableResult> finalResults = new HashMap<>(512);
        for (final Future<Map<String, MutableResult>> future : futures) {
            final Map<String, MutableResult> chunkResults = future.get();
            for (final Map.Entry<String, MutableResult> entry : chunkResults.entrySet()) {
                finalResults.compute(entry.getKey(), (key, existing) -> {
                    if (existing == null) {
                        return entry.getValue();
                    } else {
                        existing.merge(entry.getValue());
                        return existing;
                    }
                });
            }
        }

        executor.shutdown();
        channel.close();
        file.close();

        final TreeMap<String, MutableResult> sortedResults = new TreeMap<>(finalResults);

        System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println(sortedResults);
    }

    private static Map<String, MutableResult> processChunk(final FileChannel channel,
                                                           final long start,
                                                           final long end) throws Exception {

        final Map<String, MutableResult> results = new HashMap<>(512);
        long currentPos = start;

        if (start > 0) {
            currentPos = findNextLineStart(channel, start);
        }

        final int BUFFER_SIZE = 8 * 1024 * 1024; // 8 MB
        final byte[] buffer = new byte[BUFFER_SIZE];
        final byte[] overflow = new byte[256]; // Для неполных строк
        int overflowLen = 0;

        while (currentPos < end) {
            final int toRead = (int) Math.min(end - currentPos, BUFFER_SIZE - overflowLen);

            // Копируем overflow в начало буфера
            if (overflowLen > 0) {
                System.arraycopy(overflow, 0, buffer, 0, overflowLen);
            }

            final ByteBuffer bb = ByteBuffer.wrap(buffer, overflowLen, toRead);
            final int bytesRead = channel.read(bb, currentPos);

            if (bytesRead <= 0) {
                break;
            }

            final int totalBytes = overflowLen + bytesRead;
            final int processed = processBuffer(buffer, totalBytes, results, currentPos + bytesRead >= end);

            // Сохраняем необработанный остаток
            overflowLen = totalBytes - processed;
            if (overflowLen > 0 && overflowLen < overflow.length) {
                System.arraycopy(buffer, processed, overflow, 0, overflowLen);
            }

            currentPos += bytesRead;
        }

        return results;
    }

    private static long findNextLineStart(final FileChannel channel,
                                          final long position) throws Exception {
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        channel.read(buffer, position);
        buffer.flip();

        while (buffer.hasRemaining()) {
            if (buffer.get() == '\n') {
                return position + buffer.position();
            }
        }

        return position;
    }

    private static int processBuffer(final byte[] buffer,
                                     final int length,
                                     final Map<String, MutableResult> results,
                                     final boolean isLastChunk) {
        int pos = 0;
        int lastComplete = 0;

        while (pos < length) {
            // Ищем ';' с помощью SWAR
            final int semicolonPos = findByteSWAR(buffer, pos, length, (byte) ';');

            if (semicolonPos == -1) {
                break;
            }

            // Ищем '\n'
            int newlinePos = findByteSWAR(buffer, semicolonPos + 1, length, (byte) '\n');

            if (newlinePos == -1) {
                if (!isLastChunk) break;
                newlinePos = length;
            }

            // Обрабатываем строку
            final String station = new String(buffer, pos, semicolonPos - pos);
            final double temperature = parseTemperatureFast(buffer, semicolonPos + 1, newlinePos);

            MutableResult result = results.get(station);
            if (result == null) {
                result = new MutableResult();
                results.put(station, result);
            }
            result.update(temperature);

            pos = newlinePos + 1;
            lastComplete = pos;
        }

        return lastComplete;
    }

    // SWAR поиск байта (обрабатываем 8 байт за раз)
    private static int findByteSWAR(final byte[] buffer,
                                    final int start,
                                    final int end,
                                    final byte target) {
        int pos = start;

        // Скалярный поиск для небольших участков
        int remaining = end - start;
        if (remaining < 16) {
            while (pos < end) {
                if (buffer[pos] == target) return pos;
                pos++;
            }
            return -1;
        }

        // Выравниваем до 8 байт
        while (pos < end && (pos & 7) != 0) {
            if (buffer[pos] == target) return pos;
            pos++;
        }

        // SWAR: обрабатываем по 8 байт
        long targetBroadcast = (target & 0xFFL) * 0x0101010101010101L;

        while (pos + 8 <= end) {
            final long chunk = getLong(buffer, pos);
            final long xor = chunk ^ targetBroadcast;
            final long match = (xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L;

            if (match != 0) {
                // Нашли совпадение
                for (int i = 0; i < 8; i++) {
                    if (buffer[pos + i] == target) {
                        return pos + i;
                    }
                }
            }

            pos += 8;
        }

        // Остаток
        while (pos < end) {
            if (buffer[pos] == target) return pos;
            pos++;
        }

        return -1;
    }

    // Читаем 8 байт как long (big-endian)
    private static long getLong(final byte[] buffer,
                                final int offset) {
        return ((long) buffer[offset] << 56) |
                ((long) (buffer[offset + 1] & 0xFF) << 48) |
                ((long) (buffer[offset + 2] & 0xFF) << 40) |
                ((long) (buffer[offset + 3] & 0xFF) << 32) |
                ((long) (buffer[offset + 4] & 0xFF) << 24) |
                ((long) (buffer[offset + 5] & 0xFF) << 16) |
                ((long) (buffer[offset + 6] & 0xFF) << 8) |
                ((long) (buffer[offset + 7] & 0xFF));
    }

    private static double parseTemperatureFast(final byte[] buffer,
                                               final int start,
                                               final int end) {
        boolean negative = false;
        int pos = start;

        if (pos >= end) return 0.0;

        if (buffer[pos] == '-') {
            negative = true;
            pos++;
        }

        // Формат: X.X или XX.X (максимум 2 цифры до точки, 1 после)
        int intPart = 0;
        while (pos < end && buffer[pos] != '.') {
            intPart = intPart * 10 + (buffer[pos] - '0');
            pos++;
        }

        int fracPart = 0;
        if (pos < end && buffer[pos] == '.') {
            pos++;
            if (pos < end && buffer[pos] >= '0' && buffer[pos] <= '9') {
                fracPart = buffer[pos] - '0';
            }
        }

        double result = intPart + fracPart * 0.1;
        return negative ? -result : result;
    }
}
