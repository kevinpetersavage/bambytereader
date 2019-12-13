import com.google.common.base.Stopwatch;
import htsjdk.samtools.*;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.CloseableIterator;
import org.jooq.lambda.Seq;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import static org.jooq.lambda.Seq.seq;


public class Main {
    public static void main(String[] args) throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Stopwatch stopWatch = Stopwatch.createStarted();

        File bam = new File(args[0]);
        String chromosome = args[1];
        int start = Integer.parseInt(args[2]);
        int end = Integer.parseInt(args[3]);

        SamReaderFactory srf = SamReaderFactory.make();
        srf.validationStringency(ValidationStringency.LENIENT);
        SamReader samReader = srf.open(SamInputResource.of(bam));

        List<Chunk> chunks = getChunks(samReader, chromosome, start, end);

        long startOfFirstChunk = seq(chunks).map(Chunk::getChunkStart).min().get();
        long startOfLastChunk = seq(chunks).map(Chunk::getChunkStart).max().get();

        long startVirtualFilePointer = positionOfRecordNear(bam, startOfFirstChunk, start);
        long endVirtualFilePointer = positionOfRecordNear(bam, startOfLastChunk, end);


        System.out.printf("input: %d - %d%n", start, end);
        System.out.printf("virtual pointers: %d-%d%n", startVirtualFilePointer, endVirtualFilePointer);
        System.out.printf("block addresses: %d-%d%n",
                BlockCompressedFilePointerUtil.getBlockAddress(startVirtualFilePointer),
                BlockCompressedFilePointerUtil.getBlockAddress(endVirtualFilePointer) - 1
        );
        System.out.printf("block offset: %d-%d%n",
                BlockCompressedFilePointerUtil.getBlockOffset(startVirtualFilePointer),
                BlockCompressedFilePointerUtil.getBlockOffset(endVirtualFilePointer) - 1
        );

        System.out.println(stopWatch.toString());
    }

    private static long positionOfRecordNear(File bam, long start, long positionToFind) throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
        try (BlockCompressedInputStream blockCompressedInputStream = new BlockCompressedInputStream(bam)) {
            long seekInitialPos = start;

            Constructor[] declaredConstructors = BAMFileReader.class.getDeclaredConstructors();
            Constructor<BAMFileReader> constructor = Seq.of(declaredConstructors).findFirst(
                    c -> Arrays.equals(c.getParameterTypes(), new Class[]{
                            BlockCompressedInputStream.class,
                            File.class,
                            Boolean.TYPE, Boolean.TYPE,
                            String.class,
                            ValidationStringency.class,
                            SAMRecordFactory.class
                    })
            ).get();

            constructor.setAccessible(true);
            BAMFileReader instance = constructor.newInstance(
                    blockCompressedInputStream,
                    new File(bam.getAbsolutePath() + ".bai"),
                    false,
                    false,
                    "used for reporting",
                    ValidationStringency.LENIENT,
                    DefaultSAMRecordFactory.getInstance()
            );

            blockCompressedInputStream.seek(seekInitialPos);

            CloseableIterator<SAMRecord> iterator = instance.getIterator();
            SAMRecord record = iterator.next();
            while (record.getStart() < positionToFind) {
                record = iterator.next();
            }

            return blockCompressedInputStream.getPosition();
        }
    }

    private static List<Chunk> getChunks(SamReader samReader, String chromosome, int start, int end) {
        int sequenceIndex = samReader.getFileHeader().getSequenceIndex(chromosome);

        BAMIndex index = samReader.indexing().getIndex();
        return index.getSpanOverlapping(sequenceIndex, start, end).getChunks();
    }

}
