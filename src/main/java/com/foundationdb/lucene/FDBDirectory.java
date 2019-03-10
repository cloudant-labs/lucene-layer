/**
 * FoundationDB Lucene Layer
 * Copyright (c) 2013 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.foundationdb.lucene;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class FDBDirectory extends Directory
{
    /** See {@link RAMInputStream#BUFFER_SIZE} */
    private static final int BUFFER_SIZE = 1024;

    public final Tuple subspace;
    private final Tuple dirSubspace;
    private final Tuple dataSubspace;
    private Database db;
    private LockFactory lockFactory;


    public FDBDirectory(String path, Database db) {
        this(Tuple.from(Util.DEFAULT_ROOT_PREFIX, path), db);
    }

    public FDBDirectory(Tuple subspace, Database db) {
        this(subspace, db, NoLockFactory.getNoLockFactory());
    }

    FDBDirectory(Tuple subspace, Database db, LockFactory lockFactory) {
        assert subspace != null;
        assert db != null;
        assert lockFactory != null;
        this.db = db;
        this.subspace = subspace;
        this.dirSubspace = subspace.add(0);
        this.dataSubspace = subspace.add(1);
        try {
            setLockFactory(lockFactory);
        } catch(IOException e) {
            throw new IllegalStateException("setLockFactory threw", e);
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
				byte[] outValue = new byte[(int)length()];        	
				writeTo(outValue, 0);
				
				db.run(txn -> {
					Util.writeLargeValue(txn, dataSubspace.add(dataID), BUFFER_SIZE, outValue);
					return null;
				});
			} catch (IOException e) {
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
    	return (long) db.run(txn -> {
	        byte[] value = Util.get(txn.get(dirSubspace.add(name).pack()));
	        if(value == null) {
	            return -1L;
	        }
	        return Tuple.fromBytes(value).getLong(0);
    	});
    }

    private long createDataID(String name) {
    	return (long) db.run(txn -> {
	        AsyncIterator<KeyValue> it = txn.getRange(dataSubspace.range(), 1, true).iterator();
	        long nextID = 0;
	        if(it.hasNext()) {
	            KeyValue kv = it.next();
	            nextID = Tuple.fromBytes(kv.getKey()).getLong(dataSubspace.size()) + 1;
	        }
	        txn.set(dirSubspace.add(name).pack(), Tuple.from(nextID).pack());
	        txn.set(dataSubspace.add(nextID).add(0).pack(), Util.EMPTY_BYTES);
	        return nextID;
    	});
    }

    @Override
    public String[] listAll() {
    	return db.run(txn -> {
    		List<String> outList = new ArrayList<String>();
    		for(KeyValue kv : txn.getRange(dirSubspace.range())) {
    			outList.add(Tuple.fromBytes(kv.getKey()).getString(dirSubspace.size()));
    		}
    		return outList.toArray(new String[outList.size()]);
    	});
    }

    @Override
    public boolean fileExists(String name) {
        Util.specialFileExists(name, this);
        return getDataID(name) != -1;
    }

    @Override
    public void deleteFile(String name) throws NoSuchFileException {
    	long result = (long) db.run(txn -> {
	        long dataID = getDataID(name);
	        if(dataID == -1) {
	            return -1L;
	        }
	        txn.clear(dirSubspace.add(name).pack());
	        txn.clear(dataSubspace.add(dataID).range());
	        return 0L;
    	});
        if(result == -1L) {
            throw new NoSuchFileException(name);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        return openInput(name, null).length();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws FileAlreadyExistsException {
        if(getDataID(name) != -1) {
            throw new FileAlreadyExistsException(name);
        }
        return new Output(name, createDataID(name));
    }

    @Override
    public void sync(Collection<String> names) {
        // None
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        long dataID = getDataID(name);
        if(dataID == -1) {
            throw new FileNotFoundException(name);
        }
        IndexInput result = db.run(txn -> {
            InputFile file = new InputFile();
            int totalLen = 0;
	        for(KeyValue kv : txn.getRange(dataSubspace.add(dataID).range())) {
	            byte[] value = kv.getValue();
	            byte[] ramValue = file.addBufferInternal(value.length);
	            totalLen += value.length;
	            System.arraycopy(value, 0, ramValue, 0, value.length);
	        }
	        file.setLengthInternal(totalLen);
	        try {
				return new RAMInputStream(name, file);
			} catch (IOException e) {
				return null;
			}
        });
        if (result == null) {
        	throw new IOException("new RAMInputStream threw I/O exception");
        }
        return result;
    }

    @Override
    public void close() {
        // None
    }

	@Override
	public Lock makeLock(String name) {
		return lockFactory.makeLock(name);
	}

	@Override
	public void clearLock(String name) throws IOException {
		lockFactory.clearLock(name);
	}

	@Override
	public void setLockFactory(LockFactory lockFactory) throws IOException {
		this.lockFactory = lockFactory;
	}

	@Override
	public LockFactory getLockFactory() {
		return lockFactory;
	}	
	
	public <T> T run(Function<? super Transaction,T> retryable) {
		return db.run(retryable);
	}
	
	public Transaction createTransaction() {
		return db.createTransaction();
	}

}