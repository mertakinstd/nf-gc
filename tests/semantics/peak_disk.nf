nextflow.enable.dsl=2

params.publish_dir = null
params.save_intermediates = false

/*
 * Small publication-aware lifetime fixture derived from the real nf-core
 * acceptance workload.  The payloads stay tiny; only the lifetime topology is
 * important:
 *
 *   - a conditionally unpublished hard-linked intermediate with direct demand;
 *   - the same shape crossing a runtime operator before its consumer;
 *   - published siblings/results that must remain retained.
 */

process INTERMEDIATE_SOURCE {
    maxForks 1
    tag "$sample"

    publishDir params.publish_dir, mode: 'link', pattern: '*.intermediate.bin', saveAs: { filename ->
        params.save_intermediates ? filename : null
    }
    publishDir params.publish_dir, mode: 'link', pattern: '*.summary.txt'

    input:
    val sample

    output:
    tuple val(sample), path("${sample}.intermediate.bin"), emit: intermediate
    tuple val(sample), path("${sample}.summary.txt"), emit: summary

    script:
    """
    printf '%s-intermediate\n' '$sample' > ${sample}.intermediate.bin
    printf '%s-summary\n' '$sample' > ${sample}.summary.txt
    """
}

process DIRECT_CONSUMER {
    maxForks 1
    tag "$sample"

    publishDir params.publish_dir, mode: 'link'

    input:
    tuple val(sample), path(source)

    output:
    path "${sample}.direct.done"

    script:
    """
    if [[ "$sample" == "slow" ]]; then sleep 2; fi
    cat "$source" > ${sample}.direct.done
    """
}

process OPERATOR_SOURCE {
    publishDir params.publish_dir, mode: 'link', pattern: 'operator.intermediate.bin', saveAs: { filename ->
        params.save_intermediates ? filename : null
    }

    output:
    path 'operator.intermediate.bin'

    script:
    """
    echo operator > operator.intermediate.bin
    """
}

process OPERATOR_CONSUMER {
    publishDir params.publish_dir, mode: 'link'

    input:
    path source

    output:
    path 'operator.done'

    script:
    """
    cat "$source" > operator.done
    """
}

workflow {
    INTERMEDIATE_SOURCE(Channel.of('fast', 'slow'))
    DIRECT_CONSUMER(INTERMEDIATE_SOURCE.out.intermediate)

    OPERATOR_SOURCE()
    operator_route = OPERATOR_SOURCE.out.mix(Channel.empty())
    OPERATOR_CONSUMER(operator_route)
}
