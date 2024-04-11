/*
 * Copyright (c) 2024.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.modelix.model.server.store

import org.modelix.model.persistent.HashUtil

data class ObjectInRepository(private val repositoryId: String, val key: String) {
    fun isGlobal() = repositoryId == ""
    fun isImmutable() = HashUtil.isSha256(key)
    fun getRepositoryId() = repositoryId.takeIf { it != "" }

    companion object {
        fun global(key: String) = ObjectInRepository("", key)
        fun create(repositoryId: String?, key: String) = ObjectInRepository(repositoryId ?: "", key)
    }
}
