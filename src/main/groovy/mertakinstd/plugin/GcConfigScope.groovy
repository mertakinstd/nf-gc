/*
 * Copyright 2026, Mert Akin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mertakinstd.plugin

import groovy.transform.CompileStatic
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

/**
 * Declares the public nf-gc configuration scope to Nextflow.
 */
@ScopeName(GcConfig.CONFIG_SCOPE)
@CompileStatic
final class GcConfigScope implements ConfigScope {

    @ConfigOption
    @Description('Garbage-collection dependency policy: process or artifact')
    String gc_mode = GcMode.PROCESS.configValue

    GcConfigScope() {}

    GcConfigScope(Map options) {
        final Object value = options?.get(GcConfig.MODE_KEY)
        if( value != null )
            this.gc_mode = value.toString()
    }
}
