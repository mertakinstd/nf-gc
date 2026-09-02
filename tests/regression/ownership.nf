nextflow.enable.dsl=2

params.scenario = null
params.external_path = null
params.emit_optional = false

process COPY_EXTERNAL_DEFAULT {
    input:
    path external

    output:
    path 'generated.txt'

    script:
    """
    cat "$external" > generated.txt
    """
}

process COPY_EXTERNAL_STAGE_COPY {
    stageInMode 'copy'

    input:
    path external

    output:
    path 'generated.txt'

    script:
    """
    cat "$external" > generated.txt
    """
}

process GENERATED_SOURCE {
    output:
    path 'source.txt'

    script:
    """
    echo generated > source.txt
    """
}

process PASSTHROUGH_DEFAULT {
    input:
    path source

    output:
    path 'source.txt', includeInputs: true

    script:
    """
    test -f source.txt
    """
}

process PASSTHROUGH_STAGE_COPY {
    stageInMode 'copy'

    input:
    path source

    output:
    path 'source.txt', includeInputs: true

    script:
    """
    test -f source.txt
    """
}

process READ_LATE {
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

process SYMLINK_OUTPUT {
    input:
    val external_path

    output:
    path 'link.txt'

    script:
    """
    ln -s "$external_path" link.txt
    """
}

process DIRECTORY_WITH_EXTERNAL_SYMLINK {
    input:
    val external_path

    output:
    path 'bundle'

    script:
    """
    mkdir bundle
    echo local > bundle/local.txt
    ln -s "$external_path" bundle/external.txt
    """
}

process READ_LINK {
    input:
    path source

    output:
    path 'read.txt'

    script:
    """
    cat "$source" > read.txt
    """
}

process READ_DIRECTORY {
    input:
    path bundle

    output:
    path 'read.txt'

    script:
    """
    cat "$bundle/local.txt" "$bundle/external.txt" > read.txt
    """
}

process GLOB_SOURCE {
    output:
    path '*.txt'

    script:
    """
    echo one > one.txt
    echo two > two.txt
    """
}

process READ_GLOB {
    input:
    path files

    output:
    path 'combined.txt'

    script:
    """
    cat *.txt > combined.txt
    """
}

process OPTIONAL_SOURCE {
    output:
    path 'main.txt', emit: main
    path 'optional.txt', optional: true, emit: optional

    script:
    """
    echo main > main.txt
    if [[ '${params.emit_optional}' == 'true' ]]; then
        echo optional > optional.txt
    fi
    """
}

process READ_OPTIONAL_MAIN {
    input:
    path main

    output:
    path 'done.txt'

    script:
    """
    cat "$main" > done.txt
    """
}

process MULTI_OUTPUT_SOURCE {
    output:
    path 'sample.bam', emit: bam
    path 'sample.bai', emit: bai

    script:
    """
    echo bam > sample.bam
    echo bai > sample.bai
    echo tool-log > align.log
    echo scratch > scratch.tmp
    """
}

process DECLARED_LOG_SOURCE {
    output:
    path 'sample.bam', emit: bam
    path 'align.log', emit: log

    script:
    """
    echo bam > sample.bam
    echo tool-log > align.log
    """
}

process READ_PAIR {
    input:
    path bam
    path bai

    output:
    path 'done.txt'

    script:
    """
    cat "$bam" "$bai" > done.txt
    """
}

process GLOB_WITH_SIDE_SOURCE {
    output:
    path '*.bam'

    script:
    """
    echo one > one.bam
    echo two > two.bam
    echo three > three.bam
    echo tool-log > tool.log
    """
}

process READ_BAM_GLOB {
    input:
    path files

    output:
    path 'done.txt'

    script:
    """
    cat *.bam > done.txt
    """
}

process TUPLE_SOURCE {
    input:
    val sample

    output:
    tuple val(sample), path("${sample}.bam"), path("${sample}.bai")

    script:
    """
    echo bam > ${sample}.bam
    echo bai > ${sample}.bai
    """
}

process READ_TUPLE {
    input:
    tuple val(sample), path(bam), path(bai)

    output:
    path 'done.txt'

    script:
    """
    cat "$bam" "$bai" > done.txt
    """
}

process DIRECTORY_OUTPUT_SOURCE {
    output:
    path 'index'

    script:
    """
    mkdir -p index/nested
    echo index > index/data.bin
    echo nested > index/nested/data.bin
    echo tool-log > tool.log
    """
}

process READ_INDEX {
    input:
    path index

    output:
    path 'done.txt'

    script:
    """
    cat "$index/data.bin" "$index/nested/data.bin" > done.txt
    """
}

process STAGED_MULTI_SOURCE {
    input:
    path reads

    output:
    path 'sample.bam', emit: bam
    path 'sample.bai', emit: bai

    script:
    """
    cat "$reads" > sample.bam
    echo index > sample.bai
    echo tool-log > tool.log
    """
}

workflow {
    if( params.scenario in ['external_default', 'external_stage_copy', 'external_reemit', 'staged_multi_output'] ) {
        external = file(params.external_path)
        java.nio.file.Files.createDirectories(external.parent)
        java.nio.file.Files.writeString(external, 'external-data\n')
    }

    if( params.scenario == 'external_default' ) {
        COPY_EXTERNAL_DEFAULT(external)
        READ_LATE(COPY_EXTERNAL_DEFAULT.out)
    }
    else if( params.scenario == 'external_stage_copy' ) {
        COPY_EXTERNAL_STAGE_COPY(external)
        READ_LATE(COPY_EXTERNAL_STAGE_COPY.out)
    }
    else if( params.scenario == 'external_reemit' ) {
        PASSTHROUGH_DEFAULT(external)
        READ_LATE(PASSTHROUGH_DEFAULT.out)
    }
    else if( params.scenario == 'passthrough_default' ) {
        GENERATED_SOURCE()
        PASSTHROUGH_DEFAULT(GENERATED_SOURCE.out)
        READ_LATE(PASSTHROUGH_DEFAULT.out)
    }
    else if( params.scenario == 'passthrough_stage_copy' ) {
        GENERATED_SOURCE()
        PASSTHROUGH_STAGE_COPY(GENERATED_SOURCE.out)
        READ_LATE(PASSTHROUGH_STAGE_COPY.out)
    }
    else if( params.scenario == 'symlink_output' ) {
        external = file(params.external_path)
        java.nio.file.Files.createDirectories(external.parent)
        java.nio.file.Files.writeString(external, 'external-target\n')
        SYMLINK_OUTPUT(external.toString())
        READ_LINK(SYMLINK_OUTPUT.out)
    }
    else if( params.scenario == 'directory_symlink' ) {
        external = file(params.external_path)
        java.nio.file.Files.createDirectories(external.parent)
        java.nio.file.Files.writeString(external, 'external-target\n')
        DIRECTORY_WITH_EXTERNAL_SYMLINK(external.toString())
        READ_DIRECTORY(DIRECTORY_WITH_EXTERNAL_SYMLINK.out)
    }
    else if( params.scenario == 'glob_outputs' ) {
        GLOB_SOURCE()
        READ_GLOB(GLOB_SOURCE.out)
    }
    else if( params.scenario == 'optional_absent' ) {
        OPTIONAL_SOURCE()
        READ_OPTIONAL_MAIN(OPTIONAL_SOURCE.out.main)
    }
    else if( params.scenario == 'multi_output_side_files' ) {
        MULTI_OUTPUT_SOURCE()
        READ_PAIR(MULTI_OUTPUT_SOURCE.out.bam, MULTI_OUTPUT_SOURCE.out.bai)
    }
    else if( params.scenario == 'declared_log_output' ) {
        DECLARED_LOG_SOURCE()
        READ_PAIR(DECLARED_LOG_SOURCE.out.bam, DECLARED_LOG_SOURCE.out.log)
    }
    else if( params.scenario == 'glob_with_side_file' ) {
        GLOB_WITH_SIDE_SOURCE()
        READ_BAM_GLOB(GLOB_WITH_SIDE_SOURCE.out)
    }
    else if( params.scenario == 'tuple_outputs' ) {
        TUPLE_SOURCE(Channel.of('sample1'))
        READ_TUPLE(TUPLE_SOURCE.out)
    }
    else if( params.scenario == 'directory_output' ) {
        DIRECTORY_OUTPUT_SOURCE()
        READ_INDEX(DIRECTORY_OUTPUT_SOURCE.out)
    }
    else if( params.scenario == 'staged_multi_output' ) {
        STAGED_MULTI_SOURCE(external)
        READ_PAIR(STAGED_MULTI_SOURCE.out.bam, STAGED_MULTI_SOURCE.out.bai)
    }
}
