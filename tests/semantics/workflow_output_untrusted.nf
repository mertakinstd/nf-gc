nextflow.enable.dsl=2

process WORKFLOW_UNTRUSTED_SOURCE {
    output:
    path 'published.txt', emit: published
    path 'candidate.txt', emit: candidate

    script:
    """
    echo workflow-output > published.txt
    echo must-retain-globally > candidate.txt
    """
}

process WORKFLOW_UNTRUSTED_CONSUMER {
    input:
    path candidate

    output:
    path 'done.txt'

    script:
    """
    cat "$candidate" > done.txt
    """
}

workflow {
    main:
    WORKFLOW_UNTRUSTED_SOURCE()
    WORKFLOW_UNTRUSTED_CONSUMER(WORKFLOW_UNTRUSTED_SOURCE.out.candidate)
    derived = WORKFLOW_UNTRUSTED_SOURCE.out.published.map { value -> value }

    publish:
    derived_output = derived
}

output {
    derived_output {
        path 'published'
        mode 'copy'
    }
}
