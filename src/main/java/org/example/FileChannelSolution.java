package org.example;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

// 16635ms
// Abha=Result[min=-32.4, max=69.1, sum=4.35585162000002E7, count=2421972]
public class FileChannelSolution {

    record Result(double min,
                  double max,
                  double sum,
                  long count) {
        Result merge(Result other) {
            return new Result
                    (Math.min(this.min, other.min),
                            Math.max(this.max, other.max),
                            this.sum + other.sum,
                            this.count + other.count
                    );
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
        System.out.println("chunkSize: " + (chunkSize/(1024 * 1024)) + "MB");

        // ExecutorService для параллельной обработки
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final List<Future<Map<String, Result>>> futures = new ArrayList<>();

        // Разбиваем файл на чанки
        for (int i = 0; i < numThreads; i++) {
            final long startPos = i * chunkSize;
            final long endPos = (i == numThreads - 1) ? fileSize : (i + 1) * chunkSize;
            futures.add(executor.submit(() -> processChunk(channel, startPos, endPos)));
        }

        // Собираем результаты из всех потоков
        final Map<String, Result> finalResults = new HashMap<>();
        for (final Future<Map<String, Result>> future : futures) {
            final Map<String, Result> chunkResults = future.get();
            for (final Map.Entry<String, Result> entry : chunkResults.entrySet()) {
                finalResults.merge(entry.getKey(), entry.getValue(), Result::merge);
            }
        }

        executor.shutdown();
        channel.close();
        file.close();

        // Сортируем результаты
        final TreeMap<String, Result> sortedResults = new TreeMap<>(finalResults);

        System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println(sortedResults);
    }

    private static Map<String, Result> processChunk(final FileChannel channel,
                                                    final long start,
                                                    final long end) throws Exception {
        final Map<String, Result> results = new HashMap<>();

        // Максимальный размер для MappedByteBuffer - Integer.MAX_VALUE
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
        // Читаем небольшой буфер для поиска начала строки
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        channel.read(buffer, position);
        buffer.flip();

        // Ищем перенос строки
        while (buffer.hasRemaining()) {
            if (buffer.get() == '\n') {
                return position + buffer.position();
            }
        }

        return position;
    }

    private static void processBuffer(final ByteBuffer buffer,
                                      final Map<String, Result> results,
                                      final boolean isLastChunk) {
        final byte[] lineBuffer = new byte[128]; // Максимальная длина строки
        int linePos = 0;

        while (buffer.hasRemaining()) {
            byte b = buffer.get();

            if (b == '\n') {
                // Обрабатываем собранную строку
                if (linePos > 0) {
                    processLine(lineBuffer, linePos, results);
                    linePos = 0;
                }
            } else {
                lineBuffer[linePos++] = b;
            }
        }

        // Если это последний чанк и остались данные, обрабатываем
        if (isLastChunk && linePos > 0) {
            processLine(lineBuffer, linePos, results);
        }
    }

    private static void processLine(final byte[] lineBuffer,
                                    final int length,
                                    final Map<String, Result> results) {
        // Ищем разделитель ';'
        int semicolonPos = -1;
        for (int i = 0; i < length; i++) {
            if (lineBuffer[i] == ';') {
                semicolonPos = i;
                break;
            }
        }

        if (semicolonPos == -1) return; // Некорректная строка

        // Извлекаем название станции
        final String station = new String(lineBuffer, 0, semicolonPos);

        // Парсим температуру
        final double temperature = parseTemperature(lineBuffer, semicolonPos + 1, length);

        // Обновляем результаты
        results.merge(station,
                new Result(temperature, temperature, temperature, 1),
                Result::merge);
    }

    private static double parseTemperature(final byte[] buffer,
                                           final int start,
                                           final int end) {
        // Простой парсинг для формата [-]XX.X
        boolean negative = false;
        int pos = start;

        if (buffer[pos] == '-') {
            negative = true;
            pos++;
        }

        int intPart = 0;
        while (pos < end && buffer[pos] != '.') {
            intPart = intPart * 10 + (buffer[pos] - '0');
            pos++;
        }

        int fracPart = 0;
        if (pos < end && buffer[pos] == '.') {
            pos++;
            while (pos < end && buffer[pos] >= '0' && buffer[pos] <= '9') {
                fracPart = fracPart * 10 + (buffer[pos] - '0');
                pos++;
            }
        }

        final double result = intPart + fracPart / 10.0;
        return negative ? -result : result;
    }
}