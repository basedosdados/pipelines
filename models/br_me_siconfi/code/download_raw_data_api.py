"""
API-based script to download raw data from SICONFI (Sistema de Informações Contábeis e Fiscais).

This script uses the SICONFI API to download data:
https://apidatalake.tesouro.gov.br/docs/siconfi/

Features:
- Uses /entes endpoint to get list of entities
- Uses /dca endpoint to download DCA (Declaração das Contas Anuais) data
- Respects rate limit (1 request per second)
- Progress tracking for resumable downloads
- Handles pagination automatically
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

# Directory configuration
DOWNLOAD_DIR = Path("input/api")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# API rate limiting (1 request per second as per API docs)
RATE_LIMIT_DELAY = (
    1.1  # seconds between requests (slightly more than 1 to be safe)
)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Years to download (adjust range as needed)
START_YEAR = 2013
END_YEAR = datetime.now().year

# Progress tracking file
PROGRESS_FILE = DOWNLOAD_DIR / "download_progress_api.json"

# PoC configuration
POC_MODE = True  # Set to False to download all data
POC_YEAR = 2013
POC_MAX_ENTES = 5  # Number of municipalities to download in PoC mode


# ============================================================================
# HTTP Client Setup
# ============================================================================


def setup_session() -> requests.Session:
    """Set up a requests session with retry strategy."""
    session = requests.Session()

    # Retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_DELAY,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set headers
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "SICONFI-API-Downloader/1.0",
        }
    )

    return session


def rate_limit():
    """Enforce rate limit (1 request per second)."""
    time.sleep(RATE_LIMIT_DELAY)


# ============================================================================
# Progress Tracking Functions
# ============================================================================


def load_progress() -> dict:
    """Load download progress from file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"completed": [], "failed": []}


def save_progress(progress: dict):
    """Save download progress to file."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def is_downloaded(
    exercicio: int, id_ente: int, no_anexo: str | None = None
) -> bool:
    """Check if a file has already been downloaded."""
    progress = load_progress()
    key = f"{exercicio}_{id_ente}_{no_anexo or 'all'}"
    return key in progress.get("completed", [])


def mark_completed(exercicio: int, id_ente: int, no_anexo: str | None = None):
    """Mark a download as completed."""
    progress = load_progress()
    key = f"{exercicio}_{id_ente}_{no_anexo or 'all'}"
    if "completed" not in progress:
        progress["completed"] = []
    if key not in progress["completed"]:
        progress["completed"].append(key)
    save_progress(progress)


def mark_failed(
    exercicio: int, id_ente: int, no_anexo: str | None = None, error: str = ""
):
    """Mark a download as failed."""
    progress = load_progress()
    key = f"{exercicio}_{id_ente}_{no_anexo or 'all'}"
    if "failed" not in progress:
        progress["failed"] = []
    progress["failed"].append(
        {
            "key": key,
            "exercicio": exercicio,
            "id_ente": id_ente,
            "no_anexo": no_anexo,
            "error": error,
            "timestamp": datetime.now().isoformat(),
        }
    )
    save_progress(progress)


# ============================================================================
# API Functions
# ============================================================================


def get_entes(
    session: requests.Session,
    co_esfera: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """
    Get list of entities (entes) from the API.

    Args:
        session: Requests session
        co_esfera: Filter by sphere (M = Municípios, E = Estados e DF, U = União, C = Consórcio)
        limit: Maximum number of entities to return (for PoC)

    Returns:
        List of entity dictionaries with id_ente, no_ente, etc.
    """
    url = f"{API_BASE_URL}/entes"
    params = {}

    if co_esfera:
        params["co_esfera"] = co_esfera

    print("📋 Fetching entities from API...")
    print(f"   URL: {url}")
    if params:
        print(f"   Params: {params}")

    all_entes = []
    offset = 0
    page_size = 5000  # API default

    while True:
        params_with_pagination = {
            **params,
            "offset": offset,
            "limit": page_size,
        }

        rate_limit()  # Respect rate limit
        try:
            response = session.get(
                url, params=params_with_pagination, timeout=30
            )
            response.raise_for_status()

            data = response.json()

            # Handle different response formats
            if isinstance(data, dict):
                # Check if it's paginated
                if "items" in data:
                    items = data["items"]
                    all_entes.extend(items)
                    # Check if there are more pages
                    if len(items) < page_size:
                        break
                    offset += page_size
                elif "count" in data or "hasMore" in data:
                    # Paginated response
                    items = data.get("items", [])
                    all_entes.extend(items)
                    if not data.get("hasMore", False):
                        break
                    offset += page_size
                else:
                    # Single page response
                    if isinstance(data, list):
                        all_entes = data
                    else:
                        # Try to extract items from nested structure
                        for key in ["items", "data", "results"]:
                            if key in data:
                                all_entes = data[key]
                                break
                        if not all_entes:
                            all_entes = [data]
                    break
            elif isinstance(data, list):
                all_entes = data
                break
            else:
                print(f"⚠️  Unexpected response format: {type(data)}")
                break

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching entities: {e}")
            if (
                hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 404
            ):
                print(
                    "   API endpoint not found. Checking if URL is correct..."
                )
            raise

    print(f"✅ Found {len(all_entes)} entities")

    # Apply limit for PoC
    if limit and len(all_entes) > limit:
        print(f"   Limiting to {limit} entities for PoC")
        all_entes = all_entes[:limit]

    return all_entes


def get_anexos_relatorios(session: requests.Session) -> list[dict]:
    """
    Get list of available annexes (anexos) from the API.

    This helps us understand what anexos are available for DCA.
    """
    url = f"{API_BASE_URL}/anexos-relatorios"

    print("📋 Fetching available annexes from API...")

    rate_limit()
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Handle different response formats
        if isinstance(data, dict) and (
            "items" in data or isinstance(data.get("items"), list)
        ):
            return data["items"]
        elif isinstance(data, list):
            return data

        return []

    except requests.exceptions.RequestException as e:
        print(f"⚠️  Error fetching annexes: {e}")
        return []


def download_dca(
    session: requests.Session,
    exercicio: int,
    id_ente: int,
    no_anexo: str | None = None,
) -> dict | None:
    """
    Download DCA (Declaração das Contas Anuais) data for a specific entity and year.

    Args:
        session: Requests session
        exercicio: Exercise year (e.g., 2013)
        id_ente: IBGE code of the entity
        no_anexo: Optional annex name (if None, gets all anexos)

    Returns:
        JSON data or None if error
    """
    url = f"{API_BASE_URL}/dca"
    params = {
        "an_exercicio": exercicio,
        "id_ente": id_ente,
    }

    if no_anexo:
        params["no_anexo"] = no_anexo

    rate_limit()  # Respect rate limit

    try:
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        return data

    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            # No data available for this combination
            return None
        print(
            f"❌ HTTP error for exercicio={exercicio}, id_ente={id_ente}: {e}"
        )
        return None
    except requests.exceptions.RequestException as e:
        print(
            f"❌ Error downloading DCA for exercicio={exercicio}, id_ente={id_ente}: {e}"
        )
        return None


def save_dca_data(
    exercicio: int,
    id_ente: int,
    no_ente: str,
    data: dict,
    no_anexo: str | None = None,
) -> Path:
    """
    Save DCA data to a JSON file with download metadata.

    Args:
        exercicio: Exercise year
        id_ente: IBGE code of the entity
        no_ente: Name of the entity
        data: JSON data to save
        no_anexo: Optional annex name

    Returns:
        Path to saved file
    """
    # Simple filename: dca_{year}_{id_ente}.json
    filename = f"dca_{exercicio}_{id_ente}.json"
    file_path = DOWNLOAD_DIR / filename

    # Wrap data with metadata including download date
    download_date = datetime.now().isoformat()
    data_with_metadata = {
        "metadata": {
            "download_date": download_date,
            "exercicio": exercicio,
            "id_ente": id_ente,
            "no_ente": no_ente,
            "no_anexo": no_anexo,
        },
        "data": data,
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_with_metadata, f, ensure_ascii=False, indent=2)

    return file_path


# ============================================================================
# Main Download Functions
# ============================================================================


def download_all_dca(
    session: requests.Session,
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    co_esfera: str | None = "M",  # Default to municipalities
    poc_mode: bool = False,
    poc_max_entes: int = 5,
):
    """
    Main function to download all DCA data.

    Args:
        session: Requests session
        start_year: Start year for downloads
        end_year: End year for downloads
        co_esfera: Filter by sphere (M = Municípios, E = Estados, etc.)
        poc_mode: If True, only download for poc_max_entes entities
        poc_max_entes: Number of entities to download in PoC mode
    """
    print("🚀 Starting SICONFI API download process...")
    print(f"📁 Download directory: {DOWNLOAD_DIR.absolute()}")
    print(f"📅 Years: {start_year} to {end_year}")

    if poc_mode:
        print(f"🧪 PoC MODE: Limiting to {poc_max_entes} entities")
        print(f"📅 PoC Year: {start_year}")

    # Step 1: Get entities
    print("\n" + "=" * 70)
    print("Step 1: Fetching entities")
    print("=" * 70)

    limit = poc_max_entes if poc_mode else None
    entes = get_entes(session, co_esfera=co_esfera, limit=limit)

    if not entes:
        print("❌ No entities found. Exiting.")
        return

    print(f"\n📊 Processing {len(entes)} entities")
    if poc_mode:
        print("   First few entities:")
        for ente in entes[:3]:
            id_ente = ente.get("cod_ibge") or ente.get("id_ente", "N/A")
            no_ente = ente.get("ente") or ente.get("no_ente", "N/A")
            print(f"     - {no_ente} (ID: {id_ente})")

    # Step 2: (Optional) Get available annexes for reference
    print("\n" + "=" * 70)
    print("Step 2: Fetching available annexes (for reference)")
    print("=" * 70)
    anexos = get_anexos_relatorios(session)
    if anexos:
        print(f"✅ Found {len(anexos)} annex types")
        print(
            f"   Sample annexes: {[a.get('no_anexo', 'N/A') for a in anexos[:5]]}"
        )
    else:
        print(
            "⚠️  Could not fetch annexes, will download all data without filtering"
        )

    # Step 3: Download DCA data for each entity and year
    print("\n" + "=" * 70)
    print("Step 3: Downloading DCA data")
    print("=" * 70)

    years = [start_year] if poc_mode else list(range(start_year, end_year + 1))
    total_downloads = len(entes) * len(years)
    current_download = 0
    successful = 0
    failed = 0
    skipped = 0

    # Calculate estimated time
    estimated_seconds = total_downloads * RATE_LIMIT_DELAY
    estimated_hours = estimated_seconds / 3600
    print(
        f"\n⏱️  Estimated time: {estimated_hours:.2f} hours ({estimated_seconds / 60:.0f} minutes)"
    )
    print(f"   (Rate limit: {RATE_LIMIT_DELAY} seconds per request)")
    print(
        "   Progress will be saved automatically - you can resume if interrupted\n"
    )

    for ente in entes:
        # The API uses 'cod_ibge' for entity ID and 'ente' for entity name
        id_ente = ente.get("cod_ibge") or ente.get("id_ente")
        no_ente = ente.get("ente") or ente.get("no_ente", f"Ente_{id_ente}")

        if not id_ente:
            print(f"⚠️  Skipping entity without cod_ibge/id_ente: {ente}")
            continue

        # Print entity header every 10 entities or at start
        print_entity_header = (
            current_download == 0
            or current_download % (len(years) * 10) == 0
            or current_download == total_downloads
        )

        if print_entity_header:
            print(f"\n{'=' * 70}")
            print(f"Processing: {no_ente} (ID: {id_ente})")
            print(f"{'=' * 70}")

        for exercicio in years:
            current_download += 1
            progress_pct = (current_download / total_downloads) * 100

            # Show progress every 10 downloads or at milestones
            show_progress = (
                current_download % 10 == 0
                or current_download == 1
                or current_download == total_downloads
                or progress_pct % 10 < 0.1
            )

            if show_progress:
                # Estimate remaining time
                elapsed_requests = current_download
                if elapsed_requests > 0:
                    avg_time_per_request = RATE_LIMIT_DELAY
                    remaining_requests = total_downloads - current_download
                    remaining_seconds = (
                        remaining_requests * avg_time_per_request
                    )
                    remaining_hours = remaining_seconds / 3600
                    remaining_minutes = (remaining_seconds % 3600) / 60
                    time_str = (
                        f"{remaining_hours:.1f}h {remaining_minutes:.0f}m"
                        if remaining_hours >= 1
                        else f"{remaining_minutes:.0f}m"
                    )
                else:
                    time_str = "calculating..."

                print(
                    f"\n[{current_download:,}/{total_downloads:,} ({progress_pct:.1f}%)] "
                    f"Exercício: {exercicio}, Ente: {no_ente} | "
                    f"✅:{successful} ❌:{failed} ⏭️:{skipped} | "
                    f"⏱️ Remaining: ~{time_str}"
                )
            else:
                # Minimal output for non-milestone downloads
                print(
                    f"  [{current_download:,}/{total_downloads:,}] {exercicio}/{no_ente[:20]}...",
                    end="\r",
                )

            # Check if already downloaded
            if is_downloaded(exercicio, id_ente):
                print("  ⏭️  Already downloaded, skipping...")
                skipped += 1
                continue

            # Download DCA data (without filtering by anexo first - get all)
            data = download_dca(session, exercicio, id_ente, no_anexo=None)

            if data is None:
                print("  ❌ No data available or error occurred")
                mark_failed(exercicio, id_ente, error="No data or API error")
                failed += 1
                continue

            # Check if data is empty
            if isinstance(data, dict):
                items = data.get("items", [])
                if not items and "count" in data and data.get("count", 0) == 0:
                    print("  ⚠️  Empty response (no data for this combination)")
                    mark_failed(exercicio, id_ente, error="Empty response")
                    failed += 1
                    continue
            elif isinstance(data, list) and len(data) == 0:
                print("  ⚠️  Empty response (no data for this combination)")
                mark_failed(exercicio, id_ente, error="Empty response")
                failed += 1
                continue

            # Save data
            try:
                file_path = save_dca_data(exercicio, id_ente, no_ente, data)
                mark_completed(exercicio, id_ente)
                print(f"  ✅ Saved to: {file_path.name}")
                successful += 1
            except Exception as e:
                print(f"  ❌ Error saving file: {e}")
                mark_failed(exercicio, id_ente, error=str(e))
                failed += 1

    # Summary
    print("\n" + "=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏭️  Skipped: {skipped}")
    print(f"📁 Files saved to: {DOWNLOAD_DIR.absolute()}")


# ============================================================================
# Command Line Interface
# ============================================================================


def test_api_connection():
    """Test API connection by making a simple request."""
    print("🧪 Testing API connection...")
    session = setup_session()

    try:
        # Test /entes endpoint
        print("  Testing /entes endpoint...")
        rate_limit()
        response = session.get(
            f"{API_BASE_URL}/entes", params={"limit": 1}, timeout=30
        )
        response.raise_for_status()
        data = response.json()
        print(f"  ✅ /entes endpoint works! Response type: {type(data)}")

        # Debug: print response structure
        if isinstance(data, dict):
            print(f"    Response keys: {list(data.keys())}")
            if "items" in data:
                print(f"    Items count: {len(data.get('items', []))}")
                if data.get("items"):
                    print(
                        f"    First item keys: {list(data['items'][0].keys())}"
                    )
        elif isinstance(data, list):
            print(f"    List length: {len(data)}")
            if data:
                print(f"    First item keys: {list(data[0].keys())}")

        # Test /dca endpoint with a sample request
        print("  Testing /dca endpoint...")
        # Get first entity ID
        first_ente = None
        if isinstance(data, dict) and "items" in data:
            items = data.get("items", [])
            if items:
                first_ente = items[0]
        elif isinstance(data, list) and len(data) > 0:
            first_ente = data[0]

        # The API uses 'cod_ibge' for entity ID and 'ente' for entity name
        id_ente = first_ente.get("cod_ibge") or first_ente.get("id_ente")
        no_ente = first_ente.get("ente") or first_ente.get(
            "no_ente", "Unknown"
        )

        if id_ente:
            print(f"    Using sample entity: {no_ente} (ID: {id_ente})")

            rate_limit()
            response = session.get(
                f"{API_BASE_URL}/dca",
                params={"an_exercicio": 2013, "id_ente": id_ente},
                timeout=30,
            )
            if response.status_code == 200:
                print(
                    f"  ✅ /dca endpoint works! Status: {response.status_code}"
                )
            elif response.status_code == 404:
                print(
                    "  ⚠️  /dca endpoint returned 404 (no data for this combination, but endpoint works)"
                )
            else:
                print(
                    f"  ⚠️  /dca endpoint returned status: {response.status_code}"
                )
        else:
            print("  ⚠️  Could not get sample entity ID for testing /dca")

        print("\n✅ API connection test completed!")
        return True

    except Exception as e:
        print(f"  ❌ API connection test failed: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        session.close()


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Download raw data from SICONFI API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test API connection first
  python download_raw_data_api.py --test

  # Run PoC (2013, 5 municipalities)
  python download_raw_data_api.py --poc

  # Run for all municipalities, all years
  python download_raw_data_api.py --start-year 2013 --end-year 2024

  # Run for specific sphere (Estados)
  python download_raw_data_api.py --co-esfera E --start-year 2020

  # Run PoC with custom number of entities
  python download_raw_data_api.py --poc --poc-max-entes 10
        """,
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test API connection and exit",
    )
    parser.add_argument(
        "--poc",
        action="store_true",
        help="Run in PoC mode (2013, limited entities)",
    )
    parser.add_argument(
        "--poc-max-entes",
        type=int,
        default=POC_MAX_ENTES,
        help=f"Number of entities to download in PoC mode (default: {POC_MAX_ENTES})",
    )
    parser.add_argument(
        "--co-esfera",
        type=str,
        default="M",
        help="Filter by sphere: M=Municípios, E=Estados e DF, U=União, C=Consórcio (default: M)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=START_YEAR,
        help=f"Start year for downloads (default: {START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=END_YEAR,
        help=f"End year for downloads (default: {END_YEAR})",
    )
    parser.add_argument(
        "--convert-to-csv",
        action="store_true",
        help="After downloading, convert all JSON files to CSV",
    )

    args = parser.parse_args()

    # Test mode
    if args.test:
        test_api_connection()
        return

    # Set up session
    session = setup_session()

    try:
        if args.poc:
            # PoC mode: just 2013 and limited entities
            download_all_dca(
                session,
                start_year=POC_YEAR,
                end_year=POC_YEAR,
                co_esfera=args.co_esfera,
                poc_mode=True,
                poc_max_entes=args.poc_max_entes,
            )
        else:
            # Full mode
            download_all_dca(
                session,
                start_year=args.start_year,
                end_year=args.end_year,
                co_esfera=args.co_esfera,
                poc_mode=False,
            )

        # Convert to CSV if requested
        if args.convert_to_csv:
            print("\n" + "=" * 70)
            print("Converting JSON files to CSV...")
            print("=" * 70)
            try:
                import sys
                from pathlib import Path

                # Add current directory to path for import
                script_dir = Path(__file__).parent
                sys.path.insert(0, str(script_dir))
                from json_to_csv import convert_all_json_to_csv

                convert_all_json_to_csv(input_dir=DOWNLOAD_DIR)
            except ImportError as e:
                print(f"⚠️  Could not import json_to_csv module: {e}")
                print("   Run: python json_to_csv.py separately")
            except Exception as e:
                print(f"❌ Error converting to CSV: {e}")
                import traceback

                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        session.close()


if __name__ == "__main__":
    main()
