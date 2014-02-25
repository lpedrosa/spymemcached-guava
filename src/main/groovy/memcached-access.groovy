import groovy.transform.Canonical
import groovy.transform.TupleConstructor

import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.Future

import net.spy.memcached.AddrUtil
import net.spy.memcached.BinaryConnectionFactory
import net.spy.memcached.ConnectionFactory
import net.spy.memcached.MemcachedClient
import net.spy.memcached.internal.GenericCompletionListener
import net.spy.memcached.internal.GetFuture
import net.spy.memcached.internal.OperationFuture

import com.google.common.util.concurrent.ForwardingFuture
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors

@Canonical
class Deal implements Serializable {
	final String name
	final double price
	
	static final long serialVersionUID = 42L;
}

class ForwardingSpyMemcachedFuture<T> extends ForwardingFuture<T> implements ListenableFuture<T> {
	
	net.spy.memcached.internal.ListenableFuture<T, GenericCompletionListener> delegate;
	
	public ForwardingSpyMemcachedFuture(net.spy.memcached.internal.ListenableFuture<T, GenericCompletionListener> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void addListener(Runnable listener, Executor executor) {
		delegate.addListener(new GenericCompletionListener<Future<?>>() {
			@Override
			void onComplete(Future<?> future) throws Exception {
				executor.execute(listener)
			}
		})
	}

	@Override
	protected Future<T> delegate() {
		return delegate;
	}
	
}

@TupleConstructor
class CacheService {
	final MemcachedClient client
	
	ListenableFuture<Boolean> store(String key, List<Deal> value, int ttlSeconds) {
		OperationFuture<Boolean> status = client.set(key, ttlSeconds, value)
		return new ForwardingSpyMemcachedFuture<Boolean>(status)
	}
	
	ListenableFuture<List<Deal>> get(String key) {
		GetFuture<List<Deal>> deals = client.asyncGet(key)
		return new ForwardingSpyMemcachedFuture<List<Deal>>(deals)
	}
}

ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10))

def hostname = 'localhost'
def port = '11211'

ConnectionFactory cf = new BinaryConnectionFactory()

MemcachedClient client = new MemcachedClient(cf, AddrUtil.getAddresses("$hostname:$port"))

CacheService cacheService = new CacheService(client)

ListenableFuture<Boolean> status = cacheService.store('myKey', [new Deal('deal1',2.0), new Deal('deal2', 3.0)], 0)

Futures.addCallback(status, new FutureCallback<Boolean>() {
	@Override
	public void onSuccess(Boolean result) {
		println "Store status: $result"
	}
	
	@Override
	public void onFailure(Throwable throwable) {
		println throwable
	}
}, pool)

ListenableFuture<List<Deal>> deals = cacheService.get('myKey')

Futures.addCallback(deals, new FutureCallback<List<Deal>>() {
	@Override
	public void onSuccess(List<Deal> result) {
		println "Get result: $result"
	}
	
	@Override
	public void onFailure(Throwable throwable) {
		println throwable
	}
}, pool)