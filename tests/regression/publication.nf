nextflow.enable.dsl=2

params.scenario = null
params.publish_dir = null
params.publish_dir_2 = null
params.store_dir = null
params.publish_mode = 'copy'


process PUBLISH_MODE {
    publishDir params.publish_dir, mode: params.publish_mode

    output:
    path 'mode.txt'

    script:
    """
    echo ${params.publish_mode} > mode.txt
    """
}

process PUBLISH_ALL {
    publishDir params.publish_dir, mode: 'copy'

    output:
    path 'result.txt'

    script:
    """
    echo published > result.txt
    """
}

process PARTIAL_PUBLISH {
    publishDir params.publish_dir, mode: 'copy', pattern: 'published.txt'

    output:
    path '*.txt'

    script:
    """
    echo published > published.txt
    echo private > private.txt
    """
}

process MULTIPLE_PUBLISH {
    publishDir params.publish_dir, mode: 'copy', pattern: 'one.txt'
    publishDir params.publish_dir_2, mode: 'copy', pattern: 'two.txt'

    output:
    path '*.txt'

    script:
    """
    echo one > one.txt
    echo two > two.txt
    """
}

process DISABLED_PUBLISH {
    publishDir params.publish_dir, mode: 'copy', enabled: false

    output:
    path 'result.txt'

    script:
    """
    echo disabled > result.txt
    """
}

process STORE_SOURCE {
    storeDir params.store_dir

    output:
    path 'stored.txt'

    script:
    """
    echo stored > stored.txt
    """
}

process TERMINAL_SOURCE {
    output:
    path 'terminal.txt'

    script:
    """
    echo terminal > terminal.txt
    """
}

process CONSUME_ONE {
    input:
    path source

    output:
    path 'done.txt'

    script:
    """
    cat "$source" > done.txt
    """
}

process CONSUME_MANY {
    input:
    path sources

    output:
    path 'done.txt'

    script:
    """
    cat *.txt > done.txt
    """
}

workflow {
    if( params.scenario == 'publish_mode' ) {
        PUBLISH_MODE()
    }
    else if( params.scenario == 'publish_all' ) {
        PUBLISH_ALL()
        CONSUME_ONE(PUBLISH_ALL.out)
    }
    else if( params.scenario == 'partial_publish' ) {
        PARTIAL_PUBLISH()
        CONSUME_MANY(PARTIAL_PUBLISH.out)
    }
    else if( params.scenario == 'multiple_publish' ) {
        MULTIPLE_PUBLISH()
        CONSUME_MANY(MULTIPLE_PUBLISH.out)
    }
    else if( params.scenario == 'disabled_publish' ) {
        DISABLED_PUBLISH()
        CONSUME_ONE(DISABLED_PUBLISH.out)
    }
    else if( params.scenario == 'store_dir' ) {
        STORE_SOURCE()
        CONSUME_ONE(STORE_SOURCE.out)
    }
    else if( params.scenario == 'terminal' ) {
        TERMINAL_SOURCE()
    }
}
