package org.example;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

// Time: 19833ms
// Abha=Result[min=-32,4, max=69,1, sum=43558516,2, count=2421972]
public class ByteArrayKeySolution {

    // Wrapper для байтового массива как ключа HashMap
    static class ByteArrayKey {
        private final byte[] bytes;
        private final int offset;
        private final int length;
        private final int hashCode;

        ByteArrayKey(final byte[] bytes,
                     final int offset,
                     final int length) {
            // Копируем байты, чтобы избежать проблем с изменяемостью
            this.bytes = Arrays.copyOfRange(bytes, offset, offset + length);
            this.offset = 0;
            this.length = length;
            this.hashCode = computeHashCode();
        }

        private int computeHashCode() {
            int h = 0;
            for (int i = 0; i < length; i++) {
                h = 31 * h + bytes[offset + i];
            }
            return h;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ByteArrayKey other)) {
                return false;
            }
            if (this.length != other.length) {
                return false;
            }

            for (int i = 0; i < length; i++) {
                if (this.bytes[this.offset + i] != other.bytes[other.offset + i]) {
                    return false;
                }
            }
            return true;
        }

        // Для финального вывода
        String toStringValue() {
            return new String(bytes, offset, length);
        }
    }

    // Mutable результат для избежания создания новых объектов
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

        @Override
        public String toString() {
            return String.format("Result[min=%.1f, max=%.1f, sum=%.1f, count=%d]", min, max, sum, count);
        }
    }

    static void main(String[] args) throws Exception {
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

        // ExecutorService для параллельной обработки
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final List<Future<Map<ByteArrayKey, MutableResult>>> futures = new ArrayList<>();

        // Разбиваем файл на чанки
        for (int i = 0; i < numThreads; i++) {
            final long startPos = i * chunkSize;
            final long endPos = (i == numThreads - 1) ? fileSize : (i + 1) * chunkSize;

            futures.add(executor.submit(() -> processChunk(channel, startPos, endPos)));
        }

        // Собираем результаты из всех потоков
        final Map<ByteArrayKey, MutableResult> finalResults = new HashMap<>();
        for (final Future<Map<ByteArrayKey, MutableResult>> future : futures) {
            final Map<ByteArrayKey, MutableResult> chunkResults = future.get();
            for (final Map.Entry<ByteArrayKey, MutableResult> entry : chunkResults.entrySet()) {
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

        // Конвертируем ByteArrayKey в String и сортируем
        final TreeMap<String, MutableResult> sortedResults = new TreeMap<>();
        for (final Map.Entry<ByteArrayKey, MutableResult> entry : finalResults.entrySet()) {
            sortedResults.put(entry.getKey().toStringValue(), entry.getValue());
        }

        System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println(sortedResults);
    }

    private static Map<ByteArrayKey, MutableResult> processChunk(final FileChannel channel,
                                                                 final long start,
                                                                 final long end) throws Exception {
        final Map<ByteArrayKey, MutableResult> results = new HashMap<>(512); // Initial capacity hint

        // Максимальный размер для MappedByteBuffer
        final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 1024;
        long currentPos = start;

        // Если не начало файла, ищем начало следующей строки
        if (start > 0) {
            currentPos = findNextLineStart(channel, start);
        }

        while (currentPos < end) {
            final long remaining = end - currentPos;
            final int bufferSize = (int) Math.min(remaining, MAX_BUFFER_SIZE);

            // Маппим кусок файла в память
            final ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPos, bufferSize);

            // Обрабатываем буфер
            processBuffer(buffer, results, currentPos + bufferSize >= end);

            currentPos += bufferSize;
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

    private static void processBuffer(final ByteBuffer buffer,
                                      final Map<ByteArrayKey, MutableResult> results,
                                      final boolean isLastChunk) {
        final byte[] lineBuffer = new byte[128];
        int linePos = 0;

        while (buffer.hasRemaining()) {
            final byte b = buffer.get();

            if (b == '\n') {
                if (linePos > 0) {
                    processLine(lineBuffer, linePos, results);
                    linePos = 0;
                }
            } else {
                lineBuffer[linePos++] = b;
            }
        }

        if (isLastChunk && linePos > 0) {
            processLine(lineBuffer, linePos, results);
        }
    }

    private static void processLine(final byte[] lineBuffer,
                                    final int length,
                                    final Map<ByteArrayKey, MutableResult> results) {
        // Ищем разделитель ';'
        int semicolonPos = -1;
        for (int i = 0; i < length; i++) {
            if (lineBuffer[i] == ';') {
                semicolonPos = i;
                break;
            }
        }

        if (semicolonPos == -1) {
            return;
        }

        // Создаём ключ из байтов (БЕЗ создания String!)
        final ByteArrayKey stationKey = new ByteArrayKey(lineBuffer, 0, semicolonPos);

        // Парсим температуру
        final double temperature = parseTemperature(lineBuffer, semicolonPos + 1, length);

        // Обновляем или создаём результат
        MutableResult result = results.get(stationKey);
        if (result == null) {
            result = new MutableResult();
            results.put(stationKey, result);
        }
        result.update(temperature);
    }

    private static double parseTemperature(final byte[] buffer,
                                           final int start,
                                           final int end) {
        boolean negative = false;
        int pos = start;

        if (buffer[pos] == '-') {
            negative = true;
            pos++;
        }

        // Парсим целую часть
        int intPart = 0;
        while (pos < end && buffer[pos] != '.') {
            intPart = intPart * 10 + (buffer[pos] - '0');
            pos++;
        }

        // Парсим дробную часть
        int fracPart = 0;
        if (pos < end && buffer[pos] == '.') {
            pos++;
            while (pos < end && buffer[pos] >= '0' && buffer[pos] <= '9') {
                fracPart = fracPart * 10 + (buffer[pos] - '0');
                pos++;
            }
        }

        double result = intPart + fracPart / 10.0;
        return negative ? -result : result;
    }
}