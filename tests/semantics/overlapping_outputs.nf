nextflow.enable.dsl=2

params.publish_dir = null

process OVERLAPPING_SOURCE {
    publishDir params.publish_dir, mode: 'symlink', pattern: 'bundle'

    output:
    path 'bundle', emit: bundle
    path 'bundle/item.txt', emit: item
    path 'independent.txt', emit: independent

    script:
    """
    mkdir bundle
    echo item > bundle/item.txt
    echo extra > bundle/extra.txt
    echo independent > independent.txt
    """
}

process OVERLAPPING_CONSUMER {
    input:
    path bundle

    output:
    path 'bundle-read.txt'

    script:
    """
    sleep 1
    cat "$bundle/item.txt" "$bundle/extra.txt" > bundle-read.txt
    """
}

process INDEPENDENT_CONSUMER {
    input:
    path source

    output:
    path 'independent-read.txt'

    script:
    """
    cat "$source" > independent-read.txt
    """
}

workflow {
    main:
    OVERLAPPING_SOURCE()
    OVERLAPPING_CONSUMER(OVERLAPPING_SOURCE.out.bundle)
    INDEPENDENT_CONSUMER(OVERLAPPING_SOURCE.out.independent)

    publish:
    published_bundle = OVERLAPPING_SOURCE.out.bundle
}

output {
    published_bundle {
        path 'workflow'
        mode 'copy'
    }
}
