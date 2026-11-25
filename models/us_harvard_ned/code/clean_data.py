#!/usr/bin/env python3
"""Script to clean and process the National Elections Database tables."""

import pandas as pd


def clean_parliamentary_elections():
    """Clean and process parliamentary elections data."""

    print("Loading parliamentary elections data...")
    df = pd.read_stata("input/parliamentary_elections_v2.dta")

    print(f"Original shape: {df.shape}")

    # Load architecture table to get column mappings
    arch_df = pd.read_excel(
        "code/architecture_table_parliamentary_elections.xlsx"
    )

    # Create mapping from original_name to new name (excluding "(excluded)" rows)
    valid_cols = arch_df[arch_df["name"] != "(excluded)"]
    rename_map = dict(
        zip(valid_cols["original_name"], valid_cols["name"], strict=True)
    )

    # Get columns to exclude
    excluded_cols = arch_df[arch_df["name"] == "(excluded)"][
        "original_name"
    ].tolist()

    # Rename columns
    df = df.rename(columns=rename_map)

    # Drop excluded columns
    cols_to_drop = [col for col in excluded_cols if col in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        print(f"Dropped excluded columns: {cols_to_drop}")

    # Convert month from category to integer
    if "month" in df.columns:
        # Map month names to integers if they're strings
        if (
            df["month"].dtype == "object"
            or df["month"].dtype.name == "category"
        ):
            month_map = {
                "January": 1,
                "February": 2,
                "March": 3,
                "April": 4,
                "May": 5,
                "June": 6,
                "July": 7,
                "August": 8,
                "September": 9,
                "October": 10,
                "November": 11,
                "December": 12,
            }
            df["month"] = df["month"].map(month_map)
        df["month"] = df["month"].astype("Int64")  # Nullable integer

    # Convert date to string format YYYY-MM-DD
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["date"] = df["date"].replace("NaT", "")
        df["date"] = df["date"].replace(
            "", None
        )  # Empty strings to None for DATE type

    # Convert year to int64 (handle any nulls)
    if "year" in df.columns:
        df["year"] = df["year"].astype("Int64")

    # Convert total_seats to int64
    if "total_seats" in df.columns:
        df["total_seats"] = df["total_seats"].astype("Int64")

    # Convert seats columns to int64
    for i in range(1, 75):
        col = f"seats_{i}"
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # Convert seat_share columns to float64 and round to avoid floating point precision issues
    for i in range(1, 75):
        col = f"seat_share_{i}"
        if col in df.columns:
            df[col] = df[col].astype("float64")
            # Round to 2 decimal places to avoid floating point precision issues in CSV export
            df[col] = df[col].round(2)

    # Convert flag columns to boolean (0/1 -> True/False/NaN)
    flag_cols = [
        "flag_constituent",
        "flag_inconsequential",
        "flag_coup",
        "flag_vacant_seats",
        "flag_appointed",
        "flag_non_partisan",
    ]
    for col in flag_cols:
        if col in df.columns:
            # Convert 0/1 to boolean, keeping NaN as NaN
            df[col] = df[col].astype("boolean")

    # Convert flag_vacant_seats_nb and flag_appointed_nb to int64
    if "flag_vacant_seats_nb" in df.columns:
        df["flag_vacant_seats_nb"] = df["flag_vacant_seats_nb"].astype("Int64")
    if "flag_appointed_nb" in df.columns:
        df["flag_appointed_nb"] = df["flag_appointed_nb"].astype("Int64")

    # Clean string columns - strip whitespace and handle empty strings
    string_cols = df.select_dtypes(include=["object"]).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
        # Replace 'nan' string with empty string
        df[col] = df[col].replace("nan", "")
        # Empty strings stay as empty strings (not None) for STRING type

    # Ensure party columns are strings
    for i in range(1, 75):
        col = f"party_{i}"
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", "")

    # Sort by country_id, year, date
    sort_cols = ["country_id", "year", "date"]
    sort_cols = [col for col in sort_cols if col in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    # Reorder columns according to architecture table order
    arch_order = valid_cols["name"].tolist()
    existing_cols = [col for col in arch_order if col in df.columns]
    other_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + other_cols]

    print(f"Cleaned shape: {df.shape}")
    print(f"Columns: {list(df.columns[:10])}...")

    # Save to CSV
    output_path = "output/parliamentary_elections.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved to {output_path}")

    return df


def clean_presidential_elections():
    """Clean and process presidential elections data."""

    print("Loading presidential elections data...")
    df = pd.read_stata("input/presidential_elections_v2.dta")

    print(f"Original shape: {df.shape}")

    # Load architecture table to get column mappings
    arch_df = pd.read_excel(
        "code/architecture_table_presidential_elections.xlsx"
    )

    # Create mapping from original_name to new name (excluding "(excluded)" rows)
    valid_cols = arch_df[arch_df["name"] != "(excluded)"]
    rename_map = dict(
        zip(valid_cols["original_name"], valid_cols["name"], strict=True)
    )

    # Get columns to exclude
    excluded_cols = arch_df[arch_df["name"] == "(excluded)"][
        "original_name"
    ].tolist()

    # Rename columns
    df = df.rename(columns=rename_map)

    # Drop excluded columns
    cols_to_drop = [col for col in excluded_cols if col in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        print(f"Dropped excluded columns: {cols_to_drop}")

    # Convert month from category to integer
    if "month" in df.columns:
        # Map month names to integers if they're strings
        if (
            df["month"].dtype == "object"
            or df["month"].dtype.name == "category"
        ):
            month_map = {
                "January": 1,
                "February": 2,
                "March": 3,
                "April": 4,
                "May": 5,
                "June": 6,
                "July": 7,
                "August": 8,
                "September": 9,
                "October": 10,
                "November": 11,
                "December": 12,
            }
            df["month"] = df["month"].map(month_map)
        df["month"] = df["month"].astype("Int64")  # Nullable integer

    # Convert date to string format YYYY-MM-DD
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["date"] = df["date"].replace("NaT", "")
        df["date"] = df["date"].replace(
            "", None
        )  # Empty strings to None for DATE type

    # Convert year to int64
    if "year" in df.columns:
        df["year"] = df["year"].astype("Int64")

    # Convert votes columns to int64
    for i in range(1, 40):
        for vote_type in ["votes1", "votes2", "e_votes"]:
            col = f"{vote_type}_{i}"
            if col in df.columns:
                df[col] = df[col].astype("Int64")

    # Convert vote_share columns to float64 and round to avoid floating point precision issues
    for i in range(1, 40):
        for share_type in ["vote_share1", "vote_share2", "e_vote_share"]:
            col = f"{share_type}_{i}"
            if col in df.columns:
                df[col] = df[col].astype("float64")
                # Round to 2 decimal places to avoid floating point precision issues in CSV export
                df[col] = df[col].round(2)

    # Convert flag columns to boolean
    flag_cols = [
        "flag_two_round",
        "flag_inconsequential",
        "flag_coup",
        "flag_plebiscite",
        "flag_unopposed",
        "flag_indirect",
    ]
    for col in flag_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    # Clean string columns - strip whitespace and handle empty strings
    string_cols = df.select_dtypes(include=["object"]).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace("nan", "")
        # Empty strings stay as empty strings (not None) for STRING type

    # Ensure candidate and party columns are strings
    for i in range(1, 40):
        for col_type in ["candidate", "party"]:
            col = f"{col_type}_{i}"
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace("nan", "")

    # Sort by country_id, year, date
    sort_cols = ["country_id", "year", "date"]
    sort_cols = [col for col in sort_cols if col in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    # Reorder columns according to architecture table order
    arch_order = valid_cols["name"].tolist()
    existing_cols = [col for col in arch_order if col in df.columns]
    other_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + other_cols]

    print(f"Cleaned shape: {df.shape}")
    print(f"Columns: {list(df.columns[:10])}...")

    # Save to CSV
    output_path = "output/presidential_elections.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved to {output_path}")

    return df


if __name__ == "__main__":
    import os

    os.chdir(
        "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/Academic/Data/World/National Elections Database"
    )
    os.makedirs("output", exist_ok=True)

    print("=" * 80)
    print("CLEANING PARLIAMENTARY ELECTIONS DATA")
    print("=" * 80)
    clean_parliamentary_elections()

    print("\n" + "=" * 80)
    print("CLEANING PRESIDENTIAL ELECTIONS DATA")
    print("=" * 80)
    clean_presidential_elections()

    print("\nDone!")
