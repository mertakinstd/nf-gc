process WORK {
    input:
    val value

    output:
    path "${value}.txt"

    script:
    """
    echo "$value" > "${value}.txt"
    """
}
