nextflow.enable.dsl=2

params.publish_dir = null

/*
 * Shared normal-lifecycle story for nf-gc semantics.
 *
 * The branches below model only distinctions that can change artifact
 * ownership, reachability, consumption, retention, or safe reclamation.
 * Failure, cache, filesystem-error, staging-mode, and publication-mechanism
 * edge cases stay in regression fixtures because they change execution
 * mechanics rather than the normal liveness story.
 */

process LINEAR_SOURCE {
    tag "$sample"

    input:
    val sample

    output:
    tuple val(sample), path("${sample}.linear.txt"), emit: artifact

    script:
    """
    echo "$sample" > ${sample}.linear.txt
    """
}

process LINEAR_CONSUMER {
    maxForks 1
    tag "$sample"

    input:
    tuple val(sample), path(source)

    output:
    path "${sample}.linear.done"

    script:
    """
    if [[ "$sample" == "two" ]]; then sleep 2; fi
    cat "$source" > ${sample}.linear.done
    """
}

process SIBLING_SOURCE {
    output:
    path 'sibling-fast.txt', emit: fast
    path 'sibling-slow.txt', emit: slow

    script:
    """
    echo fast > sibling-fast.txt
    echo slow > sibling-slow.txt
    """
}

process SIBLING_FAST {
    input:
    path source

    output:
    path 'sibling-fast.done'

    script:
    """
    cat "$source" > sibling-fast.done
    """
}

process SIBLING_SLOW {
    input:
    path source
    path gate

    output:
    path 'sibling-slow.done'

    script:
    """
    test -f "$gate"
    cat "$source" > sibling-slow.done
    """
}

process FANOUT_SOURCE {
    tag "$sample"

    input:
    val sample

    output:
    tuple val(sample), path("${sample}.fanout.txt")

    script:
    """
    echo "$sample" > ${sample}.fanout.txt
    """
}

process FANOUT_LEFT {
    maxForks 1
    tag "$sample"

    input:
    tuple val(sample), path(source)

    output:
    path "${sample}.fanout.left.done"

    script:
    """
    if [[ "$sample" == "two" ]]; then sleep 2; fi
    cat "$source" > ${sample}.fanout.left.done
    """
}

process FANOUT_RIGHT {
    maxForks 1
    tag "$sample"

    input:
    tuple val(sample), path(source)

    output:
    path "${sample}.fanout.right.done"

    script:
    """
    if [[ "$sample" == "two" ]]; then sleep 2; fi
    cat "$source" > ${sample}.fanout.right.done
    """
}

process RELAY_SOURCE {
    tag "$sample"

    input:
    val sample

    output:
    tuple val(sample), path("${sample}.relay.txt")

    script:
    """
    echo "$sample" > ${sample}.relay.txt
    """
}

process RELAY {
    maxForks 1
    tag "$sample"

    input:
    tuple val(sample), path(source)

    output:
    path '*.relay.txt', includeInputs: true

    script:
    """
    test -f "$source"
    """
}

process RELAY_CONSUMER {
    maxForks 1
    tag "$source.baseName"

    input:
    path source

    output:
    path "consumed-${source}"

    script:
    """
    if [[ "$source" == "two.relay.txt" ]]; then sleep 2; fi
    cat "$source" > consumed-${source}
    """
}

process UNUSED_SOURCE {
    maxForks 1
    tag "$sample"

    input:
    val sample

    output:
    tuple val(sample), path("${sample}.used.txt"), emit: used
    tuple val(sample), path("${sample}.unused.txt"), emit: unused

    script:
    """
    if [[ "$sample" == "two" ]]; then sleep 2; fi
    echo used > ${sample}.used.txt
    echo unused > ${sample}.unused.txt
    """
}

process USED_CONSUMER {
    tag "$sample"

    input:
    tuple val(sample), path(source)

    output:
    path "${sample}.used.done"

    script:
    """
    cat "$source" > ${sample}.used.done
    """
}

process TERMINAL_SOURCE {
    maxForks 1
    tag "$sample"

    input:
    val sample

    output:
    path "${sample}.terminal.txt"

    script:
    """
    if [[ "$sample" == "two" ]]; then sleep 2; fi
    echo terminal > ${sample}.terminal.txt
    """
}

/* One produced artifact is broadcast into several task instances. */
process SHARED_INDEX {
    output:
    path 'shared.index'

    script:
    """
    echo index > shared.index
    """
}

process INDEX_CONSUMER {
    tag "$sample"

    input:
    val sample
    path index

    output:
    path "${sample}.indexed.done"

    script:
    """
    cat "$index" > ${sample}.indexed.done
    """
}

/* A consumer cannot exist until all source items have been collected. */
process COLLECT_SOURCE {
    tag "$sample"

    input:
    val sample

    output:
    path "${sample}.collect.txt"

    script:
    """
    echo "$sample" > ${sample}.collect.txt
    """
}

process COLLECT_CONSUMER {
    input:
    path sources

    output:
    path 'collect.done'

    script:
    """
    cat *.collect.txt > collect.done
    """
}

/* Keyed demand is realized only after matching values arrive on both sides. */
process JOIN_LEFT_SOURCE {
    tag "$key"

    input:
    val key

    output:
    tuple val(key), path("${key}.left.txt")

    script:
    """
    echo left > ${key}.left.txt
    """
}

process JOIN_RIGHT_SOURCE {
    tag "$key"

    input:
    val key

    output:
    tuple val(key), path("${key}.right.txt")

    script:
    """
    echo right > ${key}.right.txt
    """
}

process JOIN_CONSUMER {
    tag "$key"

    input:
    tuple val(key), path(left), path(right)

    output:
    path "${key}.join.done"

    script:
    """
    cat "$left" "$right" > ${key}.join.done
    """
}


/* Runtime routing: each concrete artifact is sent to exactly one branch. */
process ROUTED_SOURCE {
    maxForks 1
    tag "$route"

    input:
    val route

    output:
    tuple val(route), path("${route}.routed.txt")

    script:
    """
    echo "$route" > ${route}.routed.txt
    """
}

process ROUTED_LEFT {
    tag "$route"

    input:
    tuple val(route), path(source)

    output:
    path "${route}.routed.left.done"

    script:
    """
    cat "$source" > ${route}.routed.left.done
    """
}

process ROUTED_RIGHT {
    tag "$route"

    input:
    tuple val(route), path(source)
    path gate

    output:
    path "${route}.routed.right.done"

    script:
    """
    test -f "$gate"
    cat "$source" > ${route}.routed.right.done
    """
}

/* Only one sibling is actually published. */
process PARTIAL_PUBLISH {
    publishDir params.publish_dir, mode: 'copy', pattern: '*.published.txt'

    output:
    path 'semantic.published.txt', emit: published
    path 'semantic.ephemeral.txt', emit: ephemeral

    script:
    """
    echo published > semantic.published.txt
    echo ephemeral > semantic.ephemeral.txt
    """
}

process EPHEMERAL_CONSUMER {
    input:
    path source

    output:
    path 'semantic.ephemeral.done'

    script:
    """
    cat "$source" > semantic.ephemeral.done
    """
}

workflow {
    LINEAR_SOURCE(Channel.of('one', 'two'))
    LINEAR_CONSUMER(LINEAR_SOURCE.out.artifact)

    SIBLING_SOURCE()
    SIBLING_FAST(SIBLING_SOURCE.out.fast)
    SIBLING_SLOW(SIBLING_SOURCE.out.slow, SIBLING_FAST.out)

    FANOUT_SOURCE(Channel.of('one', 'two'))
    FANOUT_LEFT(FANOUT_SOURCE.out)
    FANOUT_RIGHT(FANOUT_SOURCE.out)

    RELAY_SOURCE(Channel.of('one', 'two'))
    RELAY(RELAY_SOURCE.out)
    RELAY_CONSUMER(RELAY.out)

    UNUSED_SOURCE(Channel.of('one', 'two'))
    USED_CONSUMER(UNUSED_SOURCE.out.used)

    TERMINAL_SOURCE(Channel.of('one', 'two'))

    SHARED_INDEX()
    shared_index = SHARED_INDEX.out
    INDEX_CONSUMER(Channel.of('one', 'two', 'three'), shared_index)

    COLLECT_SOURCE(Channel.of('one', 'two', 'three'))
    COLLECT_CONSUMER(COLLECT_SOURCE.out.collect())

    JOIN_LEFT_SOURCE(Channel.of('early', 'late'))
    join_right_keys = JOIN_LEFT_SOURCE.out.map { key, source -> key }
    JOIN_RIGHT_SOURCE(join_right_keys)
    JOIN_CONSUMER(JOIN_LEFT_SOURCE.out.join(JOIN_RIGHT_SOURCE.out))

    ROUTED_SOURCE(Channel.of('left', 'right'))
    routed = ROUTED_SOURCE.out.branch { route, source ->
        left: route == 'left'
        right: route == 'right'
    }
    ROUTED_LEFT(routed.left)
    ROUTED_RIGHT(routed.right, ROUTED_LEFT.out)

    PARTIAL_PUBLISH()
    EPHEMERAL_CONSUMER(PARTIAL_PUBLISH.out.ephemeral)
}
