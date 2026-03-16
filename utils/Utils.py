def multiple_to_single_space(text):
    if isinstance(text, dict):
        text = str(text)  # or use json.dumps(text) for readable dict formatting
    text = text.replace("\t", "").replace("\n", "")
    text = " ".join(text.split()).strip()
    return text