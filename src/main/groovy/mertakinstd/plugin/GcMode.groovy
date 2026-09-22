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
 * Artifact mode is the public default and follows concrete Path liveness. It
 * reclaims an owned artifact as soon as its producer is complete, active concrete
 * consumers are gone,
 * future legal demand is sealed, and no retention rule applies. Process mode
 * uses the same concrete Path/provenance model but advances liveness only at
 * producer/consumer process boundaries. Unknown demand falls back conservatively.
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
            return ARTIFACT

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
