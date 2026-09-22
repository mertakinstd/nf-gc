nextflow.enable.dsl=2

params.publish_dir = null

/* A topic output is a real runtime demand even when the normal DAG has no edge. */
process TOPIC_SOURCE {
    output:
    path 'topic.txt', topic: semantic_topic

    script:
    """
    echo topic-payload > topic.txt
    """
}

process TOPIC_CONSUMER {
    publishDir params.publish_dir, mode: 'link'

    input:
    val payload

    output:
    path 'topic.done'

    script:
    """
    printf '%s\n' '$payload' > topic.done
    """
}

workflow {
    TOPIC_SOURCE()

    topic_payload = channel.topic('semantic_topic')
        .map { source -> source.text.trim() }

    TOPIC_CONSUMER(topic_payload)
}
