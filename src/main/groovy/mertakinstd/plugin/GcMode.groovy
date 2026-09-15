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

/**
 * Selects the garbage-collection dependency policy.
 *
 * Process mode preserves the original conservative process-level closure.
 * Artifact mode scopes dependency closure to each producer output port while
 * remaining process-granular within that port; it never performs task-instance
 * eager reclamation.
 */
@CompileStatic
enum GcMode {

    PROCESS('process'),
    ARTIFACT('artifact')

    final String configValue

    private GcMode(String configValue) {
        this.configValue = configValue
    }

    static GcMode parse(Object value) {
        if( value == null )
            return PROCESS

        final String candidate = value.toString().trim()
        for( GcMode mode : values() ) {
            if( mode.configValue == candidate )
                return mode
        }

        final String supported = values()*.configValue.join(', ')
        throw new IllegalArgumentException(
            "Unsupported nf-gc gc_mode '${candidate}'. Supported values: ${supported}"
        )
    }
}
