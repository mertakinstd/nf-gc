package mertakinstd.plugin

import nextflow.Session
import spock.lang.Specification

/**
 * Public nf-gc configuration contract.
 */
class GcConfigTest extends Specification {

    def 'defaults gc_mode to process when nf-gc config is absent'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [:]
        }

        when:
        def config = GcConfig.from(session)

        then:
        config.mode == GcMode.PROCESS
        !config.modeExplicit
    }

    def 'defaults gc_mode to process when nfGc scope omits gc_mode'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [nfGc: [:]]
        }

        when:
        def config = GcConfig.from(session)

        then:
        config.mode == GcMode.PROCESS
        !config.modeExplicit
    }

    def 'resolves explicit process gc_mode'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [nfGc: [gc_mode: 'process']]
        }

        when:
        def config = GcConfig.from(session)

        then:
        config.mode == GcMode.PROCESS
        config.modeExplicit
    }

    def 'resolves explicit artifact gc_mode'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [nfGc: [gc_mode: 'artifact']]
        }

        when:
        def config = GcConfig.from(session)

        then:
        config.mode == GcMode.ARTIFACT
        config.modeExplicit
    }

    def 'rejects an unknown gc_mode instead of silently falling back'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [nfGc: [gc_mode: 'unknown']]
        }

        when:
        GcConfig.from(session)

        then:
        def error = thrown(IllegalArgumentException)
        error.message.contains("Unsupported nf-gc gc_mode 'unknown'")
    }

    def 'rejects a malformed nf-gc configuration scope'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [nfGc: 'process']
        }

        when:
        GcConfig.from(session)

        then:
        def error = thrown(IllegalArgumentException)
        error.message == "nf-gc config 'nfGc' must be a configuration scope"
    }
    def 'formal config scope defaults gc_mode to process'() {
        expect:
        new GcConfigScope().gc_mode == 'process'
    }

    def 'formal config scope preserves an explicit artifact mode'() {
        expect:
        new GcConfigScope([gc_mode: 'artifact']).gc_mode == 'artifact'
    }

}
