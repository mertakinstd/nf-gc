nextflow.enable.dsl=2

params.scenario = null
params.publish_dir = null
params.publish_dir_2 = null
params.store_dir = null
params.publish_mode = 'copy'
params.save_align_intermeds = false


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

process NFCORE_DEFAULT_PUBLISH {
    publishDir params.publish_dir, mode: params.publish_mode, saveAs: { filename ->
        filename == 'versions.yml' ? null : filename
    }

    output:
    path 'result.txt', emit: result
    path 'versions.yml', emit: versions

    script:
    """
    echo result > result.txt
    echo version > versions.yml
    """
}

process NFCORE_STAR_PUBLISH {
    publishDir params.publish_dir, mode: 'link', pattern: '*.{out,tab}'
    publishDir params.publish_dir, mode: 'link', pattern: '*.bam', saveAs: { filename ->
        params.save_align_intermeds ? filename : null
    }

    output:
    path 'sample.bam', emit: bam
    path 'Log.final.out', emit: log
    path 'SJ.out.tab', emit: junctions

    script:
    """
    echo bam > sample.bam
    echo final > Log.final.out
    echo junctions > SJ.out.tab
    """
}

process DISABLED_PUBLISH {
    publishDir params.publish_dir, mode: 'copy', enabled: false

    output:
    path 'disabled.txt'

    script:
    """
    echo disabled > disabled.txt
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

process OVERLAPPING_PUBLISH_TREE {
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

process SYMLINK_ALIAS_PUBLISH_TREE {
    publishDir params.publish_dir, mode: 'symlink', pattern: 'linkdir'

    output:
    path 'linkdir', emit: linkdir
    path 'real/item.txt', emit: item
    path 'independent.txt', emit: independent

    script:
    """
    mkdir real
    echo item > real/item.txt
    echo extra > real/extra.txt
    ln -s real linkdir
    echo independent > independent.txt
    """
}


process SYMLINK_BACKING_SOURCE {
    output:
    path 'source-dir'

    script:
    """
    mkdir source-dir
    echo source > source-dir/item.txt
    """
}

process SYMLINK_BACKING_PUBLISH {
    publishDir params.publish_dir, mode: 'symlink'

    input:
    path source_dir

    output:
    path 'alias'

    script:
    """
    ln -s "$source_dir" alias
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
    else if( params.scenario == 'nfcore_default_saveas' ) {
        NFCORE_DEFAULT_PUBLISH()
        CONSUME_ONE(NFCORE_DEFAULT_PUBLISH.out.result)
    }
    else if( params.scenario == 'nfcore_star_saveas' ) {
        NFCORE_STAR_PUBLISH()
        CONSUME_ONE(NFCORE_STAR_PUBLISH.out.bam)
    }
    else if( params.scenario == 'disabled_publish' ) {
        DISABLED_PUBLISH()
        CONSUME_ONE(DISABLED_PUBLISH.out)
    }
    else if( params.scenario == 'multiple_publish' ) {
        MULTIPLE_PUBLISH()
        CONSUME_MANY(MULTIPLE_PUBLISH.out)
    }
    else if( params.scenario == 'overlapping_publish_tree' ) {
        OVERLAPPING_PUBLISH_TREE()
        CONSUME_ONE(OVERLAPPING_PUBLISH_TREE.out.independent)
    }
    else if( params.scenario == 'symlink_alias_publish_tree' ) {
        SYMLINK_ALIAS_PUBLISH_TREE()
        CONSUME_ONE(SYMLINK_ALIAS_PUBLISH_TREE.out.independent)
    }
    else if( params.scenario == 'symlink_backing_publish' ) {
        SYMLINK_BACKING_SOURCE()
        SYMLINK_BACKING_PUBLISH(SYMLINK_BACKING_SOURCE.out)
    }
    else if( params.scenario == 'store_dir' ) {
        STORE_SOURCE()
        CONSUME_ONE(STORE_SOURCE.out)
    }
}
