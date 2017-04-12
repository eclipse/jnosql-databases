package org.google.storage.driver.key;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class GoogleStorageKeyValueEntityManager implements BucketManager{

	private final Storage storage;
	private final String bucket;

	public GoogleStorageKeyValueEntityManager(Storage storage, String bucket) {
		super();
		this.storage = storage;
		this.bucket = bucket;
	}

	@Override
	public <K, V> void put(K key, V value) throws NullPointerException {
		
		try(InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes("UTF_8"))){
			BlobId blobId = BlobId.of(bucket,key.toString());
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build();
			storage.create(blobInfo,inputStream);
		} catch (IOException e ) {
			e.printStackTrace();
		} 
	}

	@Override
	public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
		put(entity.getKey(),entity.getValue());
		
	}

	@Override
	public <K> void put(KeyValueEntity<K> entity, Duration ttl)
			throws NullPointerException, UnsupportedOperationException {
		throw new UnsupportedOperationException("The google storage does not support getList method");
	}

	@Override
	public <K> void put(Iterable<KeyValueEntity<K>> entities) throws NullPointerException {
		StreamSupport.stream(entities.spliterator(),false).forEach(this::put);
		
	}

	@Override
	public <K> void put(Iterable<KeyValueEntity<K>> entities, Duration ttl)
			throws NullPointerException, UnsupportedOperationException {
		throw new UnsupportedOperationException("The google storage does not support getList method");
	}

	@Override
	public <K> Optional<Value> get(K key) throws NullPointerException {
		BlobId blobId = BlobId.of(bucket,key.toString());
		Blob blob = storage.get(blobId);
		//blob.re
		
//		 Blob blob = storage.get(blobId);
//	      if (blob == null) {
//	        System.out.println("No such object");
//	        return;
//	      }
//	      PrintStream writeTo = System.out;
//	      if (downloadTo != null) {
//	        writeTo = new PrintStream(new FileOutputStream(downloadTo.toFile()));
//	      }
//	      if (blob.getSize() < 1_000_000) {
//	        // Blob is small read all its content in one request
//	        byte[] content = blob.getContent();
//	        writeTo.write(content);
//	      } else {
//	        // When Blob size is big or unknown use the blob's channel reader.
//	        try (ReadChannel reader = blob.reader()) {
//	          WritableByteChannel channel = Channels.newChannel(writeTo);
//	          ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
//	          while (reader.read(bytes) > 0) {
//	            bytes.flip();
//	            channel.write(bytes);
//	            bytes.clear();
//	          }
//	        }
//	      }
//	      if (downloadTo == null) {
//	        writeTo.println();
//	      } else {
//	        writeTo.close();
//	      }

		
		return null;
	}

	@Override
	public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
		return null;//Collectors
	}

	@Override
	public <K> void remove(K key) throws NullPointerException {
		BlobId blobId = BlobId.of(bucket,key.toString());
		storage.delete(blobId);
	}

	@Override
	public <K> void remove(Iterable<K> keys) throws NullPointerException {
		StreamSupport.stream(keys.spliterator(),false).forEach(this::remove);
	}

	@Override
	public void close() {
		
	}

}
