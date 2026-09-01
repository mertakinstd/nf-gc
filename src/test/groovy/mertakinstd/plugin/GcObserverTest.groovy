package mertakinstd.plugin

import nextflow.Session
import nextflow.trace.TraceObserverV2
import spock.lang.Specification

/**
 * Basic V2 observer factory test.
 */
class GcObserverTest extends Specification {

    def 'should create the V2 observer instance' () {
        given:
        def factory = new GcObserverFactory()
        when:
        def result = factory.create(Mock(Session))
        then:
        result.size() == 1
        result.first() instanceof GcObserver
        result.first() instanceof TraceObserverV2
    }

}
