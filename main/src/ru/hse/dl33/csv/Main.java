package ru.hse.dl33.csv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Main {

    private static class Entry implements Comparable<Entry> {
        public final String line;
        public final long timestamp;
        private final int order;

        private Entry(String line, int timestampIndex, int order) {
            this.line = line;
            StringTokenizer st = new StringTokenizer(line, ","); // faster than String.split()
            for (int i = 0; i < timestampIndex; i++) { // index == 3 ==> 4th token
                st.nextToken();
            }
            timestamp = Long.parseLong(st.nextToken());
            this.order = order;
        }

        // E1 < E2 <==> E1 is newer than E2
        @Override
        public int compareTo(Entry o) {
            return Comparator.comparing((Entry e) -> e.timestamp)
                    .thenComparing((Entry e) -> e.order)
                    .compare(this, o);
        }
    }

    // generally not caring about exceptions for now
    public static void main(String[] args) throws IOException {

        // expecting valid arguments
        String filename = args[0];
        int timestampIndex = Integer.parseInt(args[1]) - 1; // assuming user inputs '1' for first column

        PriorityQueue<Entry> maxQueue = new PriorityQueue<>(); // poll() smallest timestamp = oldest
        AtomicInteger i = new AtomicInteger(); // const variable in lambda
        // try-with-resources: stream has to be closed
        try (Stream<String> lines = Files.lines(Path.of(filename))) {
            // assuming a header in the first line
            lines
                    .skip(1)
                    .forEach((line) -> {
                        maxQueue.add(new Entry(line, timestampIndex, i.get()));
                        if (maxQueue.size() > 10) {
                            maxQueue.poll();
                        }
                        i.getAndIncrement();
                    });
        }

        // order of result is not specified; sorted is more readable though
        while (maxQueue.size() > 0) {
            System.out.println(maxQueue.poll().line);
        }

    }

}
