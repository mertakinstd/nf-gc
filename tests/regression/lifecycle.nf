nextflow.enable.dsl=2

params.scenario = null
params.abort_signal = null

process FAIL_IGNORED {
    errorStrategy 'ignore'

    output:
    path 'failed.txt'

    script:
    """
    echo partial > failed.txt
    exit 1
    """
}

process FAIL_FAST {
    errorStrategy 'terminate'

    input:
    val signal_path

    output:
    path 'failed.txt'

    script:
    """
    while [[ ! -f "$signal_path" ]]; do
        sleep 0.1
    done
    echo failed > failed.txt
    exit 1
    """
}

process ABORTED_SLOW {
    input:
    val signal_path

    output:
    path 'slow.txt'

    script:
    """
    echo started > "$signal_path"
    echo partial > slow.txt
    sleep 20
    echo complete >> slow.txt
    """
}

process RETRY_SOURCE {
    errorStrategy 'retry'
    maxRetries 1

    output:
    path 'result.txt'

    script:
    """
    echo attempt-${task.attempt} > result.txt
    if [[ ${task.attempt} -eq 1 ]]; then
        exit 1
    fi
    """
}

process RETRY_CONSUMER {
    input:
    path source

    output:
    path 'done.txt'

    script:
    """
    cat "$source" > done.txt
    """
}

process MISSING_SOURCE {
    output:
    path 'victim.txt'

    script:
    """
    echo victim > victim.txt
    """
}

process REMOVE_SOURCE {
    input:
    path victim

    output:
    path 'done.txt'

    script:
    """
    cat "$victim" > done.txt
    target=\$(readlink -f "$victim")
    rm -f "\$target"
    """
}

process DELETE_FAILURE_SOURCE {
    output:
    path 'locked'

    script:
    """
    mkdir locked
    echo protected > locked/data.txt
    chmod 0555 locked
    """
}

process READ_LOCKED {
    input:
    path locked

    output:
    path 'done.txt'

    script:
    """
    cat "$locked/data.txt" > done.txt
    """
}

workflow {
    if( params.scenario == 'failed' ) {
        FAIL_IGNORED()
    }
    else if( params.scenario == 'aborted' ) {
        signal = Channel.value(params.abort_signal)
        FAIL_FAST(signal)
        ABORTED_SLOW(signal)
    }
    else if( params.scenario == 'retry' ) {
        RETRY_SOURCE()
        RETRY_CONSUMER(RETRY_SOURCE.out)
    }
    else if( params.scenario == 'missing' ) {
        MISSING_SOURCE()
        REMOVE_SOURCE(MISSING_SOURCE.out)
    }
    else if( params.scenario == 'delete_failure' ) {
        DELETE_FAILURE_SOURCE()
        READ_LOCKED(DELETE_FAILURE_SOURCE.out)
    }
}
