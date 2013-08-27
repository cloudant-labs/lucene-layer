package com.foundationdb.lucene;

import com.foundationdb.Cluster;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class FDBDirectory extends Directory
{
    private static final String SPECIAL_STRING = new String();
    private static final Random RND = new Random();

    /** See {@link RAMInputStream#BUFFER_SIZE} */
    private static final int BUFFER_SIZE = 1024;

    public Transaction txn;
    public final Tuple subspace;

    private final Tuple dirSubspace;
    private final Tuple dataSubspace;


    //
    // TODO: This should be moved to a test-only class, e.g. FDBTestDirectory, which will init and create txn.
    //
    private static FDB fdb;
    private static Database fdbDB;

    private static ThreadGroup getSystemThreadGroup() {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        while(tg != null && !"system".equals(tg.getName())) {
            tg = tg.getParent();
        }
        return tg;
    }

    public static void initFDBForTests() {
        if(fdb == null) {
            fdb = FDB.selectAPIVersion(100);

            final ThreadGroup threadGroup = getSystemThreadGroup();
            ExecutorService executor = Executors.newCachedThreadPool(
                    new ThreadFactory()
                    {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(threadGroup, r);
                        }
                    }
            );

            fdb.startNetwork(executor);
            Cluster cluster = fdb.createCluster(null, executor);
            fdbDB = cluster.openDatabase("DB".getBytes(Charset.forName("UTF8")));
            fdbDB.run(
                    new Function<Transaction, Void>()
                    {
                        @Override
                        public Void apply(Transaction transaction) {
                            transaction.clear(Tuple.from("lucene").range());
                            return null;
                        }
                    }
            );
        }
    }

    @SuppressWarnings("unused") // Invoked via reflection by tests
    public FDBDirectory() throws IOException {
        this(String.format("%d", RND.nextInt()), null, true);
    }

    public FDBDirectory(String path, Transaction txn) throws IOException {
        this(path, txn, false);
    }

    private FDBDirectory(String path, Transaction txn, boolean doInit) throws IOException {
        if(doInit) {
            assert txn == null;
            initFDBForTests();
            txn = fdbDB.createTransaction();
        }
        this.txn = txn;
        this.subspace = Tuple.from("lucene", path);
        this.dirSubspace = subspace.add(0);
        this.dataSubspace = subspace.add(1);
        setLockFactory(NoLockFactory.getNoLockFactory());
    }

    public static FDBDirectory unwrapFDBDirectory(Directory directory) throws IOException {
        if(directory instanceof FDBDirectory) {
            return (FDBDirectory) directory;
        } else if(directory instanceof CompoundFileDirectory) {
            return unwrapFDBDirectory(((CompoundFileDirectory) directory).getDirectory());
        } else {
            try {
                directory.fileExists(SPECIAL_STRING);
            } catch(TestWorkaroundException e) {
                return e.getFDBDirectory();
            }
            throw new IllegalStateException("Expected TestWorkaroundException");
        }
    }

    static byte[] copyRange(BytesRef ref) {
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }

    static long unpackLongForAtomic(byte[] bytes, int index) {
        return (bytes[index] & 0xFFL) | (bytes[index + 1] & 0xFFL) << 8 | (bytes[index + 2] & 0xFFL) << 16 | (bytes[index + 3] & 0xFFL) << 24 | (bytes[index + 4] & 0xFFL) << 32 | (bytes[index + 5] & 0xFFL) << 40 | (bytes[index + 6] & 0xFFL) << 48 | (bytes[index + 7] & 0xFFL) << 56;
    }

    public static long unpackLongForAtomic(byte[] bytes) {
        if(bytes == null) {
            return 0;
        }
        assert bytes.length == 8 : bytes.length;
        return unpackLongForAtomic(bytes, 0);
    }

    static String tupleStr(Tuple t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        boolean first = true;
        for(Object o : t.getItems()) {
            if(!first) {
                sb.append(",");
            }
            first = false;
            if(o instanceof byte[]) {
                sb.append(ByteArrayUtil.printable((byte[]) o));
            } else {
                sb.append(o);
            }
        }
        sb.append(')');
        return sb.toString();
    }


    /**
     * Write a potentially large value into multiple values of, at most, chunkSize. Keys are formed by appending the
     * running total to baseTuple.
     */
    public static int writeLargeValue(Transaction txn, Tuple baseTuple, int chunkSize, byte[] value) {
        int chunks = 0;
        int bytesWritten = 0;
        while(bytesWritten < value.length) {
            ++chunks;
            int toWrite = Math.min(chunkSize, value.length - bytesWritten);
            txn.set(
                    baseTuple.add(bytesWritten).pack(),
                    Arrays.copyOfRange(value, bytesWritten, bytesWritten + toWrite)
            );
            bytesWritten += toWrite;
        }
        //System.out.println("Write: " + name);
        //System.out.println("  byte[" + outValue.length + "]");
        assert bytesWritten == value.length;
        return chunks;
    }

    private class TestWorkaroundException extends RuntimeException
    {
        public FDBDirectory getFDBDirectory() {
            return FDBDirectory.this;
        }
    }

    private class Output extends RAMOutputStream
    {
        private final String name;
        private final long dataID;
        private boolean doingFlush = false;

        public Output(String name, long dataID) {
            this.name = name;
            this.dataID = dataID;
        }

        @Override
        public void flush() {
            if(doingFlush) {
                return;
            }
            doingFlush = true;
            try {
                flushInternal();
            } finally {
                doingFlush = false;
            }
        }

        private void flushInternal() {
            try {
                // Sets file length
                super.flush();

                byte[] outValue = new byte[(int) length()];
                writeTo(outValue, 0);
                writeLargeValue(txn, dataSubspace.add(dataID), BUFFER_SIZE, outValue);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            flush();
        }
    }

    private static class InputFile extends RAMFile
    {
        public byte[] addBufferInternal(int size) {
            return super.addBuffer(size);
        }

        public void setLengthInternal(int length) {
            super.setLength(length);
        }
    }


    //
    // Directory
    //

    private long getDataID(String name) {
        byte[] value = txn.get(dirSubspace.add(name).pack()).get();
        if(value == null) {
            return -1;
        }
        return Tuple.fromBytes(value).getLong(0);
    }

    private long createDataID(String name) {
        AsyncIterator<KeyValue> it = txn.getRange(dataSubspace.range(), 1, true).iterator();
        long nextID = 0;
        if(it.hasNext()) {
            KeyValue kv = it.next();
            nextID = Tuple.fromBytes(kv.getKey()).getLong(dataSubspace.size()) + 1;
        }
        txn.set(dirSubspace.add(name).pack(), Tuple.from(nextID).pack());
        txn.set(dataSubspace.add(nextID).add(0).pack(), new byte[0]);
        return nextID;
    }

    @Override
    public String[] listAll() throws IOException {
        List<String> outList = new ArrayList<String>();
        for(KeyValue kv : txn.getRange(dirSubspace.range())) {
            outList.add(Tuple.fromBytes(kv.getKey()).getString(dirSubspace.size()));
        }
        return outList.toArray(new String[outList.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        //noinspection StringEquality
        if(name == SPECIAL_STRING) {
            throw new TestWorkaroundException();
        }
        return getDataID(name) != -1;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        long dataID = getDataID(name);
        if(dataID == -1) {
            throw new NoSuchFileException(name);
        }
        txn.clear(dirSubspace.add(name).pack());
        txn.clear(dataSubspace.add(dataID).range());
    }

    @Override
    public long fileLength(String name) throws IOException {
        return openInput(name, null).length();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if(getDataID(name) != -1) {
            throw new FileAlreadyExistsException(name);
        }
        return new Output(name, createDataID(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // None
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        long dataID = getDataID(name);
        if(dataID == -1) {
            throw new FileNotFoundException(name);
        }
        InputFile file = new InputFile();
        int totalLen = 0;
        for(KeyValue kv : txn.getRange(dataSubspace.add(dataID).range())) {
            byte[] value = kv.getValue();
            byte[] ramValue = file.addBufferInternal(value.length);
            totalLen += value.length;
            System.arraycopy(value, 0, ramValue, 0, value.length);
        }
        file.setLengthInternal(totalLen);
        return new RAMInputStream(name, file);
    }

    @Override
    public void close() throws IOException {
    }
}