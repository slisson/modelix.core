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

package org.modelix.model.server

import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.resources.Resources
import io.ktor.server.routing.IgnoreTrailingSlash
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.websocket.WebSockets
import org.modelix.model.client2.ModelClientV2

suspend fun ApplicationTestBuilder.createModelClient(): ModelClientV2 {
    val url = "http://localhost/v2"
    return ModelClientV2.builder().url(url).client(client).build().also { it.init() }
}

fun Application.installDefaultServerPlugins() {
    install(WebSockets)
    install(ContentNegotiation) { json() }
    install(Resources)
    install(IgnoreTrailingSlash)
}
