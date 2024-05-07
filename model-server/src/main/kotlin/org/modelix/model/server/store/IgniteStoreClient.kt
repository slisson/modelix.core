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
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import org.modelix.kotlin.utils.ContextValue
import org.modelix.model.IGenericKeyListener
import org.modelix.model.persistent.HashUtil
import org.modelix.model.server.SqlUtils
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.util.*
import javax.sql.DataSource

private val LOG = KotlinLogging.logger { }

class IgniteStoreClient(jdbcConfFile: File? = null, inmemory: Boolean = false) : IGenericStoreClient<ObjectInRepository>, AutoCloseable {

    companion object {
        private const val ENTRY_CHANGED_TOPIC = "entryChanged"
    }

    private lateinit var ignite: Ignite
    private val legacyCache: IgniteCache<String, String?>
    private val mutableObjectsCache: IgniteCache<ObjectInRepository, String?>
    private val immutableObjectsCache: IgniteCache<ObjectInRepository, String?>
    private val changeNotifier = ChangeNotifier(this)
    private val pendingChangeMessages = PendingChangeMessages {
        ignite.message().send(ENTRY_CHANGED_TOPIC, it)
    }

    /**
     * Instantiate an IgniteStoreClient
     *
     * @param jdbcConfFile adopt the configuration specified. If it is not specified, configuration
     * from ignite.xml is used
     */
    init {
        if (jdbcConfFile != null) {
            // Given that systemPropertiesMode is set to 2 (SYSTEM_PROPERTIES_MODE_OVERRIDE) in
            // ignite.xml, we can override the properties through system properties
            try {
                val properties = Properties()
                properties.load(FileReader(jdbcConfFile))
                for (pn in properties.stringPropertyNames()) {
                    if (pn.startsWith("jdbc.")) {
                        System.setProperty(pn, properties.getProperty(pn))
                    } else {
                        throw RuntimeException(
                            "Properties not related to jdbc are not permitted. Check file " +
                                jdbcConfFile.absolutePath,
                        )
                    }
                }
            } catch (e: IOException) {
                throw RuntimeException(
                    "We are unable to load the JDBC configuration from " +
                        jdbcConfFile.absolutePath,
                    e,
                )
            }
        }
        val igniteConfigName = if (inmemory) "ignite-inmemory.xml" else "ignite.xml"
        if (!inmemory) updateDatabaseSchema(igniteConfigName)
        ignite = Ignition.start(javaClass.getResource(igniteConfigName))
        legacyCache = ignite.getOrCreateCache("model")
        mutableObjectsCache = ignite.getOrCreateCache("mutable_objects")
        immutableObjectsCache = ignite.getOrCreateCache("immutable_objects")

        ignite.message().localListen(ENTRY_CHANGED_TOPIC) { nodeId: UUID?, key: Any? ->
            if (key is ObjectInRepository) {
                changeNotifier.notifyListeners(key)
            }
            true
        }
    }

    private fun updateDatabaseSchema(igniteConfigName: String) {
        val dataSource: DataSource = Ignition.loadSpringBean<DataSource>(
            IgniteStoreClient::class.java.getResource(igniteConfigName),
            "dataSource",
        )
        SqlUtils(dataSource.connection).ensureSchemaInitialization()
    }

    private fun belongsToImmutableStore(key: ObjectInRepository): Boolean {
        // Immutable objects without a repository are legacy objects
        return !key.isGlobal() && key.isImmutable()
    }

    override fun getAll(keys: Set<ObjectInRepository>): Map<ObjectInRepository, String?> {
        val mutableKeys = keys.asSequence().filter { !belongsToImmutableStore(it) }.toSet()
        val mutableEntries = if (mutableKeys.isEmpty()) {
            emptyMap()
        } else {
            val entries = mutableObjectsCache.getAll(mutableKeys)
            val foundKeys = entries.asSequence().filter { it.value != null }.map { it.key.key }.toSet()
            val missingKeys = mutableKeys.asSequence().map { it.key }.minus(foundKeys).toSet()
            val legacyEntries = legacyCache.getAll(missingKeys)
            entries + legacyEntries.mapKeys { ObjectInRepository.global(it.key) }
        }

        val immutableKeys = keys.asSequence().filter { belongsToImmutableStore(it) }.toSet()
        val immutableEntries = if (immutableKeys.isEmpty()) emptyMap() else immutableObjectsCache.getAll(immutableKeys)
        return mutableEntries + immutableEntries
    }

    override fun getAll(): Map<ObjectInRepository, String?> {
        return legacyCache.asSequence().map { ObjectInRepository.global(it.key) to it.value }
            .plus(mutableObjectsCache.asSequence().map { it.key to it.value })
            .plus(immutableObjectsCache.asSequence().map { it.key to it.value })
            .toMap()
    }

    override fun putAll(entries: Map<ObjectInRepository, String?>, silent: Boolean) {
        val mutableDeletes = entries.asSequence().filter { it.value == null && !belongsToImmutableStore(it.key) }.map { it.key }.toSet()
        val mutablePuts = entries.asSequence().filter { it.value != null && !belongsToImmutableStore(it.key) }.map { it.key to it.value }.toMap()
        val immutableDeletes = entries.asSequence().filter { it.value == null && belongsToImmutableStore(it.key) }.map { it.key }.toSet()
        val immutablePuts = entries.filter { it.value != null && belongsToImmutableStore(it.key) }

        if (immutableDeletes.isNotEmpty() || immutablePuts.isNotEmpty()) {
            fun writeImmutableObjects() {
                if (immutableDeletes.isNotEmpty()) immutableObjectsCache.removeAll(immutableDeletes)
                if (immutablePuts.isNotEmpty()) immutableObjectsCache.putAll(immutablePuts)
            }

            // Writing to immutableObjectsCache has to happen outside a transaction, otherwise this exception is thrown:
            // "Transaction spans operations on atomic cache (don't use atomic cache inside a transaction)"
            if (ignite.transactions().tx() == null) {
                writeImmutableObjects()
            } else {
                runBlocking {
                    withContext(Dispatchers.IO) {
                        writeImmutableObjects()
                    }
                }
            }
        }

        runTransaction {
            if (mutableDeletes.isNotEmpty()) {
                mutableObjectsCache.removeAll(mutableDeletes)
                legacyCache.removeAll(mutableDeletes.asSequence().filter { it.isGlobal() }.map { it.key }.toSet())
            }
            if (mutablePuts.isNotEmpty()) mutableObjectsCache.putAll(mutablePuts)
            if (!silent) {
                for (key in entries.keys) {
                    if (HashUtil.isSha256(key.key)) continue
                    pendingChangeMessages.entryChanged(key)
                }
            }
        }
    }

    override fun listen(key: ObjectInRepository, listener: IGenericKeyListener<ObjectInRepository>) {
        // Entries where the key is the SHA hash over the value are not expected to change and listening is unnecessary.
        require(!HashUtil.isSha256(key.key)) { "Listener for $key will never get notified." }

        changeNotifier.addListener(key, listener)
    }

    override fun removeListener(key: ObjectInRepository, listener: IGenericKeyListener<ObjectInRepository>) {
        changeNotifier.removeListener(key, listener)
    }

    override fun generateId(key: ObjectInRepository): Long {
        return mutableObjectsCache.invoke(key, ClientIdProcessor())
    }

    override fun <T> runTransaction(body: () -> T): T {
        val transactions = ignite.transactions()
        if (transactions.tx() == null) {
            transactions.txStart().use { tx ->
                return pendingChangeMessages.runAndFlush {
                    val result = body()
                    tx.commit()
                    result
                }
            }
        } else {
            // already in a transaction
            return body()
        }
    }

    fun dispose() {
        ignite.close()
    }

    override fun close() {
        dispose()
    }
}

class PendingChangeMessages(private val notifier: (ObjectInRepository) -> Unit) {
    private val pendingChangeMessages = ContextValue<MutableSet<ObjectInRepository>>()

    fun <R> runAndFlush(body: () -> R): R {
        val messages = HashSet<ObjectInRepository>()
        return pendingChangeMessages.computeWith(messages) {
            val result = body()
            messages.forEach { notifier(it) }
            result
        }
    }

    fun entryChanged(key: ObjectInRepository) {
        val messages = checkNotNull(pendingChangeMessages.getValueOrNull()) { "Only allowed inside PendingChangeMessages.runAndFlush" }
        messages.add(key)
    }
}

class ChangeNotifier(val store: IGenericStoreClient<ObjectInRepository>) {
    private val changeNotifiers = HashMap<ObjectInRepository, EntryChangeNotifier>()

    @Synchronized
    fun notifyListeners(key: ObjectInRepository) {
        changeNotifiers[key]?.notifyIfChanged()
    }

    @Synchronized
    fun addListener(key: ObjectInRepository, listener: IGenericKeyListener<ObjectInRepository>) {
        changeNotifiers.getOrPut(key) { EntryChangeNotifier(key) }.listeners.add(listener)
    }

    @Synchronized
    fun removeListener(key: ObjectInRepository, listener: IGenericKeyListener<ObjectInRepository>) {
        val notifier = changeNotifiers[key] ?: return
        notifier.listeners.remove(listener)
        if (notifier.listeners.isEmpty()) {
            changeNotifiers.remove(key)
        }
    }

    private inner class EntryChangeNotifier(val key: ObjectInRepository) {
        val listeners = HashSet<IGenericKeyListener<ObjectInRepository>>()
        private var lastNotifiedValue: String? = null

        fun notifyIfChanged() {
            val value = store.get(key)
            if (value == lastNotifiedValue) return
            lastNotifiedValue = value

            for (listener in listeners) {
                try {
                    listener.changed(key, value)
                } catch (ex: Exception) {
                    LOG.error("Exception in listener of $key", ex)
                }
            }
        }
    }
}
