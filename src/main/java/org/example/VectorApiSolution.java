package org.example;

import java.lang.foreign.*;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

import jdk.incubator.vector.*;

// Vector API + MemorySegment solution for 1 Billion Row Challenge
// Time: 8742ms
// Abha=-32,4/18,0/69,1
public class VectorApiSolution {

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

    // Mutable результат для in-place агрегации
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

    public static void main(final String[] args) throws Exception {
        final long start = System.currentTimeMillis();

        final Path filePath = Path.of("./measurements.txt");

        // Modern Java: Arena-based MemorySegment for file mapping
        // Arena.ofShared() allows multi-threaded access (ofConfined() is single-threaded only)
        try (Arena arena = Arena.ofShared();
             FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {

            final long fileSize = channel.size();
            final MemorySegment fileSegment = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileSize,
                    arena
            );

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

                futures.add(executor.submit(() -> processChunk(fileSegment, startPos, endPos)));
            }

            // Собираем результаты из всех потоков
            final Map<ByteArrayKey, MutableResult> finalResults = new HashMap<>(512);
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

            // Конвертируем ByteArrayKey в String и сортируем
            final TreeMap<String, MutableResult> sortedResults = new TreeMap<>();
            for (final Map.Entry<ByteArrayKey, MutableResult> entry : finalResults.entrySet()) {
                sortedResults.put(entry.getKey().toStringValue(), entry.getValue());
            }

            System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
            System.out.println(sortedResults);
        }
    }

    private static Map<ByteArrayKey, MutableResult> processChunk(final MemorySegment fileSegment,
                                                                 final long start,
                                                                 final long end) throws Exception {
        final Map<ByteArrayKey, MutableResult> results = new HashMap<>(512);
        long currentPos = start;

        // Если не начало файла, ищем начало следующей строки
        if (start > 0) {
            currentPos = findNextLineStart(fileSegment, start, end);
        }

        // Обрабатываем строки
        while (currentPos < end) {
            // Используем Vector API для поиска разделителя ';'
            long semicolonPos = findByteVector(fileSegment, currentPos, end, (byte) ';');
            if (semicolonPos == -1) {
                break;
            }

            // Ищем '\n' после разделителя
            long newlinePos = findByteVector(fileSegment, semicolonPos + 1, end, (byte) '\n');
            if (newlinePos == -1) {
                // Строка не закончена (граница чанка)
                break;
            }

            // Извлекаем название станции
            final int stationLen = (int) (semicolonPos - currentPos);
            final byte[] stationBytes = new byte[stationLen];
            MemorySegment.copy(fileSegment, currentPos,
                    MemorySegment.ofArray(stationBytes), 0,
                    stationLen);
            final ByteArrayKey station = new ByteArrayKey(stationBytes, 0, stationLen);

            // Парсим температуру
            final double temperature = parseTemperatureFast(fileSegment, semicolonPos + 1, newlinePos);

            // Обновляем результаты
            MutableResult result = results.get(station);
            if (result == null) {
                result = new MutableResult();
                results.put(station, result);
            }
            result.update(temperature);

            // Переходим к следующей строке
            currentPos = newlinePos + 1;
        }

        return results;
    }

    // Vector API: Поиск байта с использованием SIMD
    private static long findByteVector(final MemorySegment segment,
                                       final long start,
                                       final long end,
                                       final byte target) {
        final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;
        final int LANES = SPECIES.length();

        long pos = start;
        final ByteVector targetVec = ByteVector.broadcast(SPECIES, target);

        // Vector loop: обрабатываем SPECIES.length() байт за итерацию
        final long vectorEnd = end - (end - start) % LANES;

        while (pos + LANES <= vectorEnd) {
            final ByteVector chunk = ByteVector.fromMemorySegment(
                    SPECIES,
                    segment,
                    pos,
                    ByteOrder.nativeOrder()
            );

            final VectorMask<Byte> matches = chunk.eq(targetVec);
            if (matches.anyTrue()) {
                return pos + matches.firstTrue();
            }

            pos += LANES;
        }

        // Scalar fallback для остатка
        while (pos < end) {
            if (segment.get(ValueLayout.JAVA_BYTE, pos) == target) {
                return pos;
            }
            pos++;
        }

        return -1;
    }

    // Найти начало следующей строки (для выравнивания чанков)
    private static long findNextLineStart(final MemorySegment segment,
                                          final long position,
                                          final long end) {
        // Ищем '\n' и возвращаем позицию после него
        long pos = position;
        while (pos < end) {
            if (segment.get(ValueLayout.JAVA_BYTE, pos) == '\n') {
                return pos + 1;
            }
            pos++;
        }
        return position;
    }

    // Оптимизированный парсинг температуры из MemorySegment
    // Формат: [-]X.X или [-]XX.X
    private static double parseTemperatureFast(final MemorySegment segment,
                                               final long start,
                                               final long end) {
        if (start >= end) return 0.0;

        long pos = start;
        boolean negative = false;

        // Проверяем знак
        if (segment.get(ValueLayout.JAVA_BYTE, pos) == '-') {
            negative = true;
            pos++;
        }

        // Парсим целую часть
        int intPart = 0;
        while (pos < end && segment.get(ValueLayout.JAVA_BYTE, pos) != '.') {
            intPart = intPart * 10 + (segment.get(ValueLayout.JAVA_BYTE, pos) - '0');
            pos++;
        }

        // Парсим дробную часть
        int fracPart = 0;
        if (pos < end && segment.get(ValueLayout.JAVA_BYTE, pos) == '.') {
            pos++;
            if (pos < end) {
                byte digit = segment.get(ValueLayout.JAVA_BYTE, pos);
                if (digit >= '0' && digit <= '9') {
                    fracPart = digit - '0';
                }
            }
        }

        double result = intPart + fracPart * 0.1;
        return negative ? -result : result;
    }
}
