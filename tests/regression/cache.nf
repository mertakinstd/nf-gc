nextflow.enable.dsl=2

params.store_dir = null

process STORED_CACHE {
    storeDir params.store_dir

    output:
    path 'cached.txt'

    script:
    """
    echo executed > cached.txt
    """
}

process CACHE_CONSUMER {
    input:
    path cached

    output:
    path 'done.txt'

    script:
    """
    cat "$cached" > done.txt
    """
}

workflow {
    cacheDir = file(params.store_dir)
    java.nio.file.Files.createDirectories(cacheDir)
    java.nio.file.Files.writeString(cacheDir.resolve('cached.txt'), 'prebuilt-cache\n')

    STORED_CACHE()
    CACHE_CONSUMER(STORED_CACHE.out)
}
