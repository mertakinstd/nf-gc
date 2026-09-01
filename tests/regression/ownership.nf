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

workflow {
    if( params.scenario in ['external_default', 'external_stage_copy', 'external_reemit'] ) {
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
}
