package org.example;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

// 6191ms
// Abha=-32,4/18,0/69,1
public class SwarAndThreadLocalMaps {

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

        final int numThreads = Runtime.getRuntime().availableProcessors();
        System.out.println("Количество потоков: " + numThreads);

        // Thread-local результаты для каждого потока
        final List<Map<String, MutableResult>> threadLocalResults = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final Thread[] threads = new Thread[numThreads];

        // Запускаем потоки
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            final long startPos = i * (fileSize / numThreads);
            final long endPos = (i == numThreads - 1) ? fileSize : (i + 1) * (fileSize / numThreads);

            threads[i] = new Thread(() -> {
                try {
                    final Map<String, MutableResult> localResults = processChunk(channel, startPos, endPos, threadId);
                    threadLocalResults.add(localResults);
                } catch (final Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }, "Worker-" + i);

            threads[i].start();
        }

        // Ждём завершения всех потоков
        latch.await();

        channel.close();
        file.close();

        System.out.println("Processing complete, merging results...");

        // Merge всех thread-local результатов
        final Map<String, MutableResult> finalResults = new HashMap<>(1024, 0.75f);

        for (final Map<String, MutableResult> threadResults : threadLocalResults) {
            for (final Map.Entry<String, MutableResult> entry : threadResults.entrySet()) {
                final String station = entry.getKey();
                final MutableResult threadResult = entry.getValue();

                final MutableResult existing = finalResults.get(station);
                if (existing == null) {
                    finalResults.put(station, threadResult);
                } else {
                    existing.merge(threadResult);
                }
            }
        }

        // Сортируем результаты
        final TreeMap<String, MutableResult> sortedResults = new TreeMap<>(finalResults);

        final long elapsed = System.currentTimeMillis() - start;
        System.out.println("Time: " + elapsed + "ms");
        System.out.println("Stations found: " + sortedResults.size());
        System.out.println(sortedResults);
    }

    private static Map<String, MutableResult> processChunk(final FileChannel channel,
                                                           final long start,
                                                           final long end,
                                                           final int threadId) throws Exception {

        // Thread-local HashMap с хорошим initial capacity
        final Map<String, MutableResult> results = new HashMap<>(1024, 0.75f);

        long currentPos = start;

        // Если не начало файла, ищем начало следующей строки
        if (start > 0) {
            currentPos = findNextLineStart(channel, start);
        }

        // Буфер для чтения (8 MB на поток)
        final int BUFFER_SIZE = 8 * 1024 * 1024;
        final byte[] buffer = new byte[BUFFER_SIZE];
        final byte[] overflow = new byte[256]; // Для неполных строк на границах буфера
        int overflowLen = 0;

        long totalProcessed = 0;

        while (currentPos < end) {
            int toRead = (int) Math.min(end - currentPos, BUFFER_SIZE - overflowLen);

            // Копируем overflow из предыдущей итерации в начало буфера
            if (overflowLen > 0) {
                System.arraycopy(overflow, 0, buffer, 0, overflowLen);
            }

            // Читаем данные из файла
            ByteBuffer bb = ByteBuffer.wrap(buffer, overflowLen, toRead);
            int bytesRead = channel.read(bb, currentPos);

            if (bytesRead <= 0) break;

            int totalBytes = overflowLen + bytesRead;

            // Обрабатываем буфер
            int processed = processBuffer(buffer, totalBytes, results, currentPos + bytesRead >= end);

            totalProcessed += processed - overflowLen;

            // Сохраняем необработанный остаток для следующей итерации
            overflowLen = totalBytes - processed;
            if (overflowLen > 0 && overflowLen < overflow.length) {
                System.arraycopy(buffer, processed, overflow, 0, overflowLen);
            } else if (overflowLen >= overflow.length) {
                // Строка слишком длинная, пропускаем
                overflowLen = 0;
            }

            currentPos += bytesRead;
        }

        if (threadId == 0) {
            System.out.println("Thread " + threadId + " processed " + totalProcessed + " bytes, found " + results.size() + " stations");
        }

        return results;
    }

    private static long findNextLineStart(final FileChannel channel,
                                          final long position) throws Exception {
        final ByteBuffer buffer = ByteBuffer.allocate(256);
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
            int lineStart = pos;

            // Ищем ';' с помощью SWAR
            int semicolonPos = findByteSWAR(buffer, pos, length, (byte) ';');

            if (semicolonPos == -1) {
                // Не нашли разделитель в оставшейся части
                break;
            }

            // Ищем '\n' после разделителя
            int newlinePos = findByteSWAR(buffer, semicolonPos + 1, length, (byte) '\n');

            if (newlinePos == -1) {
                // Строка не закончена
                if (!isLastChunk) {
                    // Оставляем для следующего буфера
                    break;
                }
                // Последний chunk - обрабатываем до конца
                newlinePos = length;
            }

            // Извлекаем название станции
            int stationLen = semicolonPos - lineStart;
            String station = new String(buffer, lineStart, stationLen);

            // Парсим температуру
            double temperature = parseTemperatureFast(buffer, semicolonPos + 1, newlinePos);

            // Обновляем результаты
            MutableResult result = results.get(station);
            if (result == null) {
                result = new MutableResult();
                results.put(station, result);
            }
            result.update(temperature);

            // Переходим к следующей строке
            pos = newlinePos + 1;
            lastComplete = pos;
        }

        return lastComplete;
    }

    // SWAR (SIMD Within A Register) поиск байта
    private static int findByteSWAR(final byte[] buffer,
                                    final int start,
                                    final int end,
                                    final byte target) {
        int pos = start;

        // Для коротких участков используем скалярный поиск
        int remaining = end - start;
        if (remaining < 16) {
            while (pos < end) {
                if (buffer[pos] == target) return pos;
                pos++;
            }
            return -1;
        }

        // Выравниваем позицию до 8 байт для оптимального доступа
        while (pos < end && (pos & 7) != 0) {
            if (buffer[pos] == target) return pos;
            pos++;
        }

        // Создаём паттерн: повторяем искомый байт 8 раз в long
        // Например, для ';' (0x3B): 0x3B3B3B3B3B3B3B3B
        final long targetBroadcast = (target & 0xFFL) * 0x0101010101010101L;

        // SWAR цикл: обрабатываем по 8 байт за итерацию
        while (pos + 8 <= end) {
            // Читаем 8 байт как long
            final long chunk = getLongBigEndian(buffer, pos);

            // XOR с паттерном: если байт совпадает с target, результат будет 0
            final long xor = chunk ^ targetBroadcast;

            // Магическая формула для определения нулевых байтов:
            // Если байт == 0, то в соответствующей позиции установится бит 0x80
            final long match = (xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L;

            if (match != 0) {
                // Нашли совпадение где-то в этих 8 байтах
                // Ищем точную позицию скалярно
                for (int i = 0; i < 8; i++) {
                    if (buffer[pos + i] == target) {
                        return pos + i;
                    }
                }
            }

            pos += 8;
        }

        // Обрабатываем остаток скалярно
        while (pos < end) {
            if (buffer[pos] == target) return pos;
            pos++;
        }

        return -1;
    }

    // Читаем 8 байт как long (big-endian для правильного порядка байтов)
    private static long getLongBigEndian(final byte[] buffer,
                                         final int offset) {
        return ((long) buffer[offset] << 56) |
                (((long) buffer[offset + 1] & 0xFF) << 48) |
                (((long) buffer[offset + 2] & 0xFF) << 40) |
                (((long) buffer[offset + 3] & 0xFF) << 32) |
                (((long) buffer[offset + 4] & 0xFF) << 24) |
                (((long) buffer[offset + 5] & 0xFF) << 16) |
                (((long) buffer[offset + 6] & 0xFF) << 8) |
                ((long) buffer[offset + 7] & 0xFF);
    }

    // Оптимизированный парсинг температуры
    // Формат: [-]X.X или [-]XX.X (максимум 2 цифры до точки, 1 после)
    private static double parseTemperatureFast(final byte[] buffer,
                                               final int start,
                                               final int end) {
        if (start >= end) {
            return 0.0;
        }

        int pos = start;
        boolean negative = false;

        // Проверяем знак
        if (buffer[pos] == '-') {
            negative = true;
            pos++;
        }

        // Парсим целую часть (1-2 цифры)
        int intPart = 0;
        while (pos < end && buffer[pos] != '.') {
            byte b = buffer[pos];
            if (b >= '0' && b <= '9') {
                intPart = intPart * 10 + (b - '0');
                pos++;
            } else {
                break;
            }
        }

        // Пропускаем точку
        if (pos < end && buffer[pos] == '.') {
            pos++;
        }

        // Парсим дробную часть (1 цифра)
        int fracPart = 0;
        if (pos < end) {
            byte b = buffer[pos];
            if (b >= '0' && b <= '9') {
                fracPart = b - '0';
            }
        }

        // Собираем результат: умножение на 0.1 быстрее деления на 10
        double result = intPart + fracPart * 0.1;

        return negative ? -result : result;
    }
}