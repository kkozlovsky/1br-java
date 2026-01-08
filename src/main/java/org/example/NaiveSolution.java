package org.example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

// 204847 ms
// Abha=Result[min=-32.4, max=69.1, sum=4.3558516200000644E7, count=2421972
public class NaiveSolution {

    record Result(double min, double max, double sum, long count) {
    }

    static void main(String[] args) throws FileNotFoundException {
        final long start = System.currentTimeMillis();
        final ConcurrentSkipListMap<String, Result> results = new BufferedReader(new FileReader("./measurements.txt"))
                .lines()
                .map(l -> l.split(";"))
                .collect(Collectors.toMap(
                        parts -> parts[0],
                        parts -> {
                            final double temperature = Double.parseDouble(parts[1]);
                            return new Result(temperature, temperature, temperature, 1);
                        },
                        (oldResult, newResult) -> {
                            final double min = Math.min(oldResult.min, newResult.min);
                            final double max = Math.max(oldResult.max, newResult.max);
                            final double sum = oldResult.sum + newResult.sum;
                            final long count = oldResult.count + newResult.count;
                            return new Result(min, max, sum, count);
                        }, ConcurrentSkipListMap::new));
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(results);
    }

}
