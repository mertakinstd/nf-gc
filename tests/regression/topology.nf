nextflow.enable.dsl=2

include { WORK as WORK_A; WORK as WORK_B } from './modules/worker'

params.scenario = null

process DUAL_SOURCE {
    output:
    path 'fast.txt', emit: fast
    path 'slow.txt', emit: slow

    script:
    """
    echo fast > fast.txt
    echo slow > slow.txt
    """
}

process FAST_CONSUMER {
    input:
    path source

    output:
    path 'fast.done'

    script:
    """
    sleep 1
    cat "$source" > fast.done
    """
}

process SLOW_CONSUMER {
    input:
    path source

    output:
    path 'slow.done'

    script:
    """
    sleep 3
    cat "$source" > slow.done
    """
}

process INNER_SINK {
    input:
    path source

    output:
    path 'inner.done'

    script:
    """
    cat "$source" > inner.done
    """
}

process OUTER_SINK {
    input:
    path source

    output:
    path 'outer.done'

    script:
    """
    cat "$source" > outer.done
    """
}

process ZERO_TASK {
    input:
    val value

    output:
    path 'zero.txt'

    script:
    """
    echo "$value" > zero.txt
    """
}

process ZERO_SINK {
    input:
    path source

    output:
    path 'zero.done'

    script:
    """
    cat "$source" > zero.done
    """
}

workflow INNER {
    take:
    values

    main:
    WORK_A(values)
    INNER_SINK(WORK_A.out)

    emit:
    result = INNER_SINK.out
}

workflow {
    if( params.scenario == 'dual_port' ) {
        DUAL_SOURCE()
        FAST_CONSUMER(DUAL_SOURCE.out.fast)
        SLOW_CONSUMER(DUAL_SOURCE.out.slow)
    }
    else if( params.scenario == 'scoped_aliases' ) {
        INNER(Channel.of('inner'))
        WORK_B(Channel.of('outer'))
        OUTER_SINK(WORK_B.out)
    }
    else if( params.scenario == 'empty_channel' ) {
        ZERO_TASK(Channel.empty())
        ZERO_SINK(ZERO_TASK.out)
    }
}
