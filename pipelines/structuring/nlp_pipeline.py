from __future__ import annotations

from typing import Any

import spacy


def load_nlp() -> Any:
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        nlp = spacy.blank("en")

    if "entity_ruler" not in nlp.pipe_names:
        ruler = nlp.add_pipe("entity_ruler")
        patterns = [
            {"label": "MINERAL", "pattern": [{"LOWER": {"IN": ["gold", "copper", "silver", "iron"]}}]},
            {"label": "ZONE", "pattern": [{"TEXT": {"REGEX": r"Zone\\s[A-Z0-9-]+"}}]},
            {"label": "EQUIPMENT", "pattern": [{"TEXT": {"REGEX": r"[A-Z]+-\\d{2,4}"}}]},
        ]
        ruler.add_patterns(patterns)

    return nlp


def extract_entities(nlp: Any, text: str) -> list[tuple[str, str]]:
    doc = nlp(text)
    return [(ent.text, ent.label_) for ent in doc.ents]
