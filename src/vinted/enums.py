from typing import Literal


Domain = Literal[
    "pl",
    "fr",
    "at",
    "be",
    "cz",
    "de",
    "dk",
    "es",
    "fi",
    "gr",
    "hr",
    "hu",
    "it",
    "lt",
    "lu",
    "nl",
    "pt",
    "ro",
    "se",
    "sk",
    "co.uk",
    "com",
]

SortOption = Literal[
    "relevance", "price_high_to_low", "price_low_to_high", "newest_first"
]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
