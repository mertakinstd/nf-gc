nextflow.enable.dsl=2

params.extra_qc = true

process BUILD_INDEX {
    input:
    path reference

    output:
    path 'index', emit: index

    script:
    """
    mkdir index
    cp "$reference" index/reference.fa
    echo ready > index/manifest.txt
    """
}

process TRIM_READS {
    input:
    tuple val(sample), path(reads)

    output:
    tuple val(sample), path("${sample}.trimmed.fastq"), emit: reads

    script:
    """
    cp "$reads" "${sample}.trimmed.fastq"
    """
}

process ALIGN {
    input:
    tuple val(sample), path(reads)
    path index

    output:
    tuple val(sample), path("${sample}.genome.bam"), emit: genome_bam
    tuple val(sample), path("${sample}.transcript.bam"), emit: transcript_bam

    script:
    """
    test -f "$index/manifest.txt"
    printf 'genome\t%s\n' "$sample" > "${sample}.genome.bam"
    printf 'transcript\t%s\n' "$sample" > "${sample}.transcript.bam"
    """
}

process SORT {
    input:
    tuple val(sample), path(bam)

    output:
    tuple val(sample), path("${sample}.sorted.bam"), emit: sorted_bam

    script:
    """
    sleep 1
    cp "$bam" "${sample}.sorted.bam"
    """
}

process ALIGN_QC {
    input:
    tuple val(sample), path(bam)

    output:
    tuple val(sample), path("${sample}.align_qc.txt"), emit: report

    script:
    """
    sleep 2
    printf 'align_qc\t%s\n' "$sample" > "${sample}.align_qc.txt"
    """
}

process QUANT {
    input:
    tuple val(sample), path(transcript_bam)
    path index

    output:
    tuple val(sample), path("${sample}.quant.txt"), emit: quant

    script:
    """
    sleep 3
    test -f "$index/manifest.txt"
    printf 'quant\t%s\n' "$sample" > "${sample}.quant.txt"
    """
}

process FEATURE_COUNTS {
    input:
    tuple val(sample), path(bam)

    output:
    tuple val(sample), path("${sample}.counts.txt"), emit: counts

    script:
    """
    sleep 1
    printf 'counts\t%s\n' "$sample" > "${sample}.counts.txt"
    """
}

process EXTRA_QC {
    input:
    tuple val(sample), path(bam)

    output:
    tuple val(sample), path("${sample}.extra_qc.txt"), emit: report

    script:
    """
    sleep 2
    printf 'extra_qc\t%s\n' "$sample" > "${sample}.extra_qc.txt"
    """
}

process MERGE_COUNTS {
    input:
    tuple val(sample), path(counts), path(quant)

    output:
    tuple val(sample), path("${sample}.merged.txt"), emit: merged

    script:
    """
    cat "$counts" "$quant" > "${sample}.merged.txt"
    """
}

process REPORT {
    input:
    path reports

    output:
    path 'report.txt'

    script:
    """
    cat *.txt > report.txt
    """
}

workflow {
    reference = file("${projectDir}/data/reference.fa", checkIfExists: true)
    raw_reads = Channel
        .fromPath("${projectDir}/data/sample*.fastq", checkIfExists: true)
        .map { reads -> tuple(reads.baseName, reads) }

    BUILD_INDEX(reference)
    TRIM_READS(raw_reads)

    ALIGN(TRIM_READS.out.reads, BUILD_INDEX.out.index)

    SORT(ALIGN.out.genome_bam)
    ALIGN_QC(ALIGN.out.genome_bam)
    QUANT(ALIGN.out.transcript_bam, BUILD_INDEX.out.index)
    FEATURE_COUNTS(SORT.out.sorted_bam)

    merged_inputs = FEATURE_COUNTS.out.counts.join(QUANT.out.quant)
    MERGE_COUNTS(merged_inputs)

    report_inputs = MERGE_COUNTS.out.merged
        .map { sample, report -> report }
        .mix(ALIGN_QC.out.report.map { sample, report -> report })

    if( params.extra_qc ) {
        EXTRA_QC(SORT.out.sorted_bam)
        report_inputs = report_inputs.mix(
            EXTRA_QC.out.report.map { sample, report -> report }
        )
    }

    REPORT(report_inputs.collect())
}
