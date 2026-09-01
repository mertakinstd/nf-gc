nextflow.enable.dsl=2

process WORKFLOW_SOURCE {
    output:
    path 'source.txt'

    script:
    """
    echo workflow-output > source.txt
    """
}

process WORKFLOW_CONSUMER {
    input:
    path source

    output:
    path 'consumed.txt'

    script:
    """
    sleep 2
    cat "$source" > consumed.txt
    """
}

workflow {
    main:
    WORKFLOW_SOURCE()
    WORKFLOW_CONSUMER(WORKFLOW_SOURCE.out)

    publish:
    published_source = WORKFLOW_SOURCE.out
}

output {
    published_source {
        path 'published'
        mode 'copy'
    }
}
