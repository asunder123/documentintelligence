# decision/chain_builder.py
# Build causal chains from ordered sentences

def build_chains(sentences_with_roles):
    """
    sentences_with_roles:
    [(sentence, role), ...]

    Returns:
    List of chains like:
    { "CAUSE": ..., "ACTION": ..., "OUTCOME": ... }
    """

    chains = []
    current = {}

    for sentence, role in sentences_with_roles:
        if role == "CAUSE":
            if current:
                chains.append(current)
            current = {"CAUSE": sentence}

        elif role in ("ACTION", "OUTCOME"):
            current[role] = sentence

    if current:
        chains.append(current)

    return chains
