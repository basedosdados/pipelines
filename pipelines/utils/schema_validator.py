"""
Implementing Pydantic Schema validators to catch changes in upstream schema
Deffining table schemas in constants.py is a common practice in our repo
This validator should be easily integrated within several pipelines
"""

from pydantic import BaseModel, root_validator


class TableSchemaValidator(BaseModel):
    source_columns: set[str]
    expected_columns: set[str]

    @root_validator(pre=False)
    @classmethod
    def validate_columns(cls, values):
        source = values.get("source_columns")
        expected = values.get("expected_columns")

        missing = expected - source

        if missing:
            raise ValueError(
                f"The schema has columns that were expected but were not found in the source table. There could have been an upstream schema change. The following columns are in the table schema registed in constants.py but arent in the downloaded table {missing}"
            )

        extra = source - expected

        if extra:
            raise ValueError(
                f"There are columns in the source schema being ignorated. {extra}"
            )

        return values


def validate_schema(source_columns: list[str], expected_columns: list[str]):
    """
    Validates that the source columns match the expected columns exactly.
    Crashes if there are missing or extra columns.
    """
    TableSchemaValidator(
        source_columns=set(source_columns),
        expected_columns=set(expected_columns),
    )
