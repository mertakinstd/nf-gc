nextflow.enable.dsl=2

process WORKFLOW_SOURCE {
    output:
    path 'final.txt', emit: final_output
    path 'intermediate.txt', emit: intermediate

    script:
    """
    echo final > final.txt
    echo intermediate > intermediate.txt
    """
}

process WORKFLOW_CONSUMER {
    input:
    path source

    output:
    path 'consumed.txt'

    script:
    """
    cat "$source" > consumed.txt
    """
}

workflow {
    main:
    WORKFLOW_SOURCE()
    WORKFLOW_CONSUMER(WORKFLOW_SOURCE.out.intermediate)

    publish:
    published_final = WORKFLOW_SOURCE.out.final_output
}

output {
    published_final {
        path 'published'
        mode 'copy'
    }
}
