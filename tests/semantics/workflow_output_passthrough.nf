nextflow.enable.dsl=2

process WORKFLOW_PASSTHROUGH_SOURCE {
    output:
    path 'source.txt'

    script:
    """
    echo retained-through-relay > source.txt
    """
}

process WORKFLOW_PASSTHROUGH_RELAY {
    input:
    path source

    output:
    path '*.txt', includeInputs: true

    script:
    """
    test -f "$source"
    """
}

workflow {
    main:
    WORKFLOW_PASSTHROUGH_SOURCE()
    WORKFLOW_PASSTHROUGH_RELAY(WORKFLOW_PASSTHROUGH_SOURCE.out)

    publish:
    published_source = WORKFLOW_PASSTHROUGH_RELAY.out
}

output {
    published_source {
        path 'published'
        mode 'copy'
    }
}
