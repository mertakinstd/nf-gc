nextflow.enable.dsl=2

process PRODUCER {
    input:
    val sample

    output:
    path "${sample}.txt"

    script:
    """
    echo "$sample" > ${sample}.txt
    """
}

process LEFT {
    input:
    path input_file

    output:
    path "left-${input_file}"

    script:
    """
    sleep 1
    cp "$input_file" "left-${input_file}"
    """
}

process RIGHT {
    input:
    path input_file

    output:
    path "right-${input_file}"

    script:
    """
    sleep 2
    cp "$input_file" "right-${input_file}"
    """
}

workflow {
    samples = Channel.of('a', 'b', 'c')

    produced = PRODUCER(samples)

    LEFT(produced)
    RIGHT(produced)
}
