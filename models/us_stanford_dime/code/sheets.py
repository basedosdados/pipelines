"""Google Sheets holding the trilingual architecture, one per table.

``bulk_upsert_columns(table_id=..., architecture_url=...)`` reads these directly.
They live in Drive under Conjuntos/us_stanford_dime and are the reviewable
artefact for the column design; ``architecture.py`` plus ``i18n.py`` remain the
generating source, and ``gen_architecture_sheets.py`` re-renders them.

Each table gets its own spreadsheet rather than a tab in a shared one: the
uploader ignores a ``#gid=`` fragment and always reads the first tab.
"""

FOLDER_ID = "1EdqEB-ISPNCWhzdsJ4YTzcXg0wjoN3L8"

SHEET_IDS = {
    "contribution": "1Ltnc79a2VFcw5nfkH6QBFm2LAT2C3i0opJnUu8r2IZY",
    "recipient": "1PMU7RyT4e5oOvy_isStPotReJZenc6MSA0sZR5I2XcA",
    "contributor": "1z0lgPbAMLvf8kVQdTN_zYTGeIi7p1ZtVl0RXpO7p0CA",
    "contributor_cycle": "1BKzlQv6qzL8oCHSmpIDodzGmawczPseqAE7rCiWRqWY",
    "dicionario": "1WrwM3I-14cNKpqq6hAFw9ziX0-HlmRhM17wxAPFC37I",
}


def url(table: str) -> str:
    """Return the architecture sheet URL for one table."""
    return f"https://docs.google.com/spreadsheets/d/{SHEET_IDS[table]}/edit"
