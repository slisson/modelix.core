/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.modelix.model.server.store

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.modelix.kotlin.utils.ContextValue
import org.modelix.model.IGenericKeyListener
import org.modelix.model.lazy.RepositoryId
import org.modelix.model.persistent.HashUtil
import java.io.File
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

/**
 * Stores objects of a hash tree. Valid entries are required to meet the condition `key == HashUtil.sha256(value)`.
 */
interface IObjectStore {
    fun getAll(key: Set<String>): Map<String, String>
    fun putAll(entries: Map<String, String?>)
}

interface IGenericStoreClient<KeyT> : AutoCloseable {
    operator fun get(key: KeyT): String? = getAll(listOf(key)).first()
    fun getAll(keys: List<KeyT>): List<String?> {
        val entries = getAll(keys.toSet())
        return keys.map { entries[it] }
    }
    fun getAll(keys: Set<KeyT>): Map<KeyT, String?>
    fun getAll(): Map<KeyT, String?>
    fun put(key: KeyT, value: String?, silent: Boolean = false) = putAll(mapOf(key to value))
    fun putAll(entries: Map<KeyT, String?>, silent: Boolean = false)
    fun listen(key: KeyT, listener: IGenericKeyListener<KeyT>)
    fun removeListener(key: KeyT, listener: IGenericKeyListener<KeyT>)
    fun generateId(key: KeyT): Long
    fun <T> runTransaction(body: () -> T): T
}

interface IStoreClient : IGenericStoreClient<String>
typealias IIsolatedStore = IGenericStoreClient<ObjectInRepository>

class RepositoryScopedStoreClient(private val repositoryId: RepositoryId, client: IGenericStoreClient<ObjectInRepository>) : StoreClientAdapter(client) {
    override fun getRepositoryId() = repositoryId
}

class GlobalStoreClient(client: IGenericStoreClient<ObjectInRepository>) : StoreClientAdapter(client) {
    override fun getRepositoryId() = null
}

class ContextScopedStoreClient(client: IGenericStoreClient<ObjectInRepository>) : StoreClientAdapter(client) {
    companion object {
        val CONTEXT_REPOSITORY = ContextValue<RepositoryId?>()
    }

    override fun getRepositoryId() = CONTEXT_REPOSITORY.getValue()
}

abstract class StoreClientAdapter(val client: IGenericStoreClient<ObjectInRepository>) : IStoreClient {

    abstract fun getRepositoryId(): RepositoryId?

    private fun String.withRepoScope(): ObjectInRepository {
        return if (HashUtil.isSha256(this)) {
            val id = getRepositoryId()
            // checkNotNull(id) { "Repository required for key $this" }
            ObjectInRepository.create(id?.id, this)
        } else {
            ObjectInRepository.global(this)
        }
    }

    override fun get(key: String): String? {
        return getAll(setOf(key))[key]
    }

    override fun getAll(keys: List<String>): List<String?> {
        val map = getAll(keys.toSet())
        return keys.map { map[it] }
    }

    override fun getAll(keys: Set<String>): Map<String, String?> {
        val fromRepository = client.getAll(keys.map { it.withRepoScope() }.toSet()).mapKeys { it.key.key }
        if (getRepositoryId() == null) return fromRepository

        // Existing databases may have objects stored without information about the repository.
        // Try to load these legacy entries.
        val missingKeys = fromRepository.entries.asSequence().filter { it.value == null }.map { ObjectInRepository.global(it.key) }.toSet()
        val fromGlobal = client.getAll(missingKeys).mapKeys { it.key.key }

        return fromRepository + fromGlobal
    }

    override fun getAll(): Map<String, String?> {
        throw UnsupportedOperationException()
        // return client.getAll().filterKeys { it.repositoryId == null || it.repositoryId == repositoryId?.id }.mapKeys { it.key.key }
    }

    override fun put(key: String, value: String?, silent: Boolean) {
        client.put(key.withRepoScope(), value, silent)
    }

    override fun putAll(entries: Map<String, String?>, silent: Boolean) {
        client.putAll(entries.mapKeys { it.key.withRepoScope() }, silent)
    }

    override fun listen(key: String, listener: IGenericKeyListener<String>) {
        client.listen(key.withRepoScope(), RepositoryScopedKeyListener(listener))
    }

    override fun removeListener(key: String, listener: IGenericKeyListener<String>) {
        client.removeListener(key.withRepoScope(), RepositoryScopedKeyListener(listener))
    }

    override fun generateId(key: String): Long {
        return client.generateId(key.withRepoScope())
    }

    override fun <T> runTransaction(body: () -> T): T {
        return client.runTransaction(body)
    }

    override fun close() {
        client.close()
    }
}

fun IGenericStoreClient<ObjectInRepository>.forRepository(repository: RepositoryId): RepositoryScopedStoreClient {
    return RepositoryScopedStoreClient(repository, this)
}
fun IGenericStoreClient<ObjectInRepository>.forGlobalRepository() = GlobalStoreClient(this)
fun IGenericStoreClient<ObjectInRepository>.forContextRepository() = ContextScopedStoreClient(this)
fun <R> IStoreClient.withGlobalRepository(body: () -> R): R = withRepository(null, body)
fun <R> IStoreClient.withRepository(repository: RepositoryId?, body: () -> R): R {
    assert(this is ContextScopedStoreClient || this is StoreClientAdapter && this.getRepositoryId() == repository) {
        "Store is not context scoped: $this"
    }
    return ContextScopedStoreClient.CONTEXT_REPOSITORY.computeWith(repository, body)
}
suspend fun <R> IStoreClient.withGlobalRepositoryInCoroutine(body: suspend () -> R): R = withRepositoryInCoroutine(null, body)

suspend fun <R> IStoreClient.withRepositoryInCoroutine(repository: RepositoryId?, body: suspend () -> R): R {
    assert(this is ContextScopedStoreClient || this is StoreClientAdapter && this.getRepositoryId() == repository) {
        "Store is not context scoped: $this"
    }
    return ContextScopedStoreClient.CONTEXT_REPOSITORY.runInCoroutine(repository, body)
}
fun IStoreClient.forRepository(repository: RepositoryId): IStoreClient {
    return RepositoryScopedStoreClient(repository, getGenericStore())
}
fun IStoreClient.getGenericStore(): IIsolatedStore {
    return (this as StoreClientAdapter).client
}

data class RepositoryScopedKeyListener(val listener: IGenericKeyListener<String>) : IGenericKeyListener<ObjectInRepository> {
    override fun changed(key: ObjectInRepository, value: String?) {
        listener.changed(key.key, value)
    }
}

suspend fun <T> IStoreClient.runTransactionSuspendable(body: () -> T): T {
    return withContext(Dispatchers.IO) { runTransaction(body) }
}

suspend fun pollEntry(storeClient: IIsolatedStore, key: ObjectInRepository, lastKnownValue: String?): String? {
    var result: String? = null
    coroutineScope {
        var handlerCalled = false
        val callHandler: suspend (String?) -> Unit = {
            handlerCalled = true
            result = it
        }

        val channel = Channel<Unit>(Channel.RENDEZVOUS)

        val listener = object : IGenericKeyListener<ObjectInRepository> {
            override fun changed(key_: ObjectInRepository, newValue: String?) {
                launch {
                    callHandler(newValue)
                    channel.trySend(Unit)
                }
            }
        }
        try {
            storeClient.listen(key, listener)
            if (lastKnownValue != null) {
                // This could be done before registering the listener, but
                // then we have to check it twice,
                // because the value could change between the first read and
                // registering the listener.
                // Most of the time the value will be equal to the last
                // known value.
                // Registering the listener without needing it is less
                // likely to happen.
                val value = storeClient[key]
                if (value != lastKnownValue) {
                    callHandler(value)
                    return@coroutineScope
                }
            }
            withTimeoutOrNull(25.seconds) {
                channel.receive() // wait until the listener is called
            }
        } finally {
            storeClient.removeListener(key, listener)
        }
        if (!handlerCalled) result = storeClient[key]
    }
    return result
}

private const val REPOSITORY_SEPARATOR = "@@"

fun IGenericStoreClient<ObjectInRepository>.loadDump(file: File): Int {
    var n = 0
    file.useLines { lines ->
        val entries = lines.associate { line ->
            val parts = line.split("#", limit = 2)
            n++
            parts[0] to parts[1]
        }.mapKeys {
            if (it.key.contains(REPOSITORY_SEPARATOR)) {
                ObjectInRepository(
                    it.key.substringAfterLast(REPOSITORY_SEPARATOR),
                    it.key.substringBeforeLast(
                        REPOSITORY_SEPARATOR,
                    ),
                )
            } else {
                ObjectInRepository.global(it.key)
            }
        }
        putAll(entries, silent = true)
    }
    return n
}

@Synchronized
@Throws(IOException::class)
fun IGenericStoreClient<ObjectInRepository>.writeDump(file: File) {
    file.writer().use { writer ->
        for ((key, value) in getAll()) {
            if (value == null) continue
            writer.append(key.key)
            if (!key.isGlobal()) {
                writer.append(REPOSITORY_SEPARATOR)
                writer.append(key.getRepositoryId())
            }
            writer.append("#")
            writer.append(value)
            writer.append("\n")
        }
    }
}
