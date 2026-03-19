"""
Web scraping script to download raw data from SICONFI (Sistema de Informações Contábeis e Fiscais).

This script uses Selenium to automate the download process from:
https://siconfi.tesouro.gov.br/siconfi/pages/public/consulta_finbra/finbra_list.jsf

Features:
- Handles captcha/puzzle challenges with manual intervention
- Retry logic with configurable delays
- Progress tracking for resumable downloads
- Automatic element detection with fallback strategies
- Downloads all files across all Exercícios and Tabelas
"""

import argparse
import json
import random
import time
import traceback
from datetime import datetime
from pathlib import Path

import undetected_chromedriver as uc
from selenium import webdriver
from selenium.common.exceptions import (
    ElementNotInteractableException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

# ============================================================================
# Configuration
# ============================================================================

URL = "https://siconfi.tesouro.gov.br/siconfi/pages/public/consulta_finbra/finbra_list.jsf"

# Directory configuration
DOWNLOAD_DIR = Path("input")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
CAPTCHA_WAIT_TIME = 120  # seconds to wait for manual captcha solving
ELEMENT_WAIT_TIMEOUT = 30  # seconds
ACTION_DELAY = 3  # seconds between actions (increased to be more human-like)
SECURITY_ERROR_DELAY = 60  # seconds to wait when security error is detected

# Human-like behavior configuration
MIN_HUMAN_DELAY = 0.5  # minimum delay in seconds
MAX_HUMAN_DELAY = 2.0  # maximum delay in seconds
MOUSE_MOVEMENT_ENABLED = True  # enable human-like mouse movements

# Years to download (adjust range as needed)
START_YEAR = 2013
END_YEAR = datetime.now().year

# Progress tracking file
PROGRESS_FILE = DOWNLOAD_DIR / "download_progress.json"


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


def is_downloaded(exercicio: str, tabela: str) -> bool:
    """Check if a file has already been downloaded."""
    progress = load_progress()
    key = f"{exercicio}_{tabela}"
    return key in progress.get("completed", [])


def mark_completed(exercicio: str, tabela: str):
    """Mark a download as completed."""
    progress = load_progress()
    key = f"{exercicio}_{tabela}"
    if "completed" not in progress:
        progress["completed"] = []
    if key not in progress["completed"]:
        progress["completed"].append(key)
    save_progress(progress)


def mark_failed(exercicio: str, tabela: str, error: str):
    """Mark a download as failed."""
    progress = load_progress()
    key = f"{exercicio}_{tabela}"
    if "failed" not in progress:
        progress["failed"] = []
    progress["failed"].append(
        {"key": key, "error": error, "timestamp": datetime.now().isoformat()}
    )
    save_progress(progress)


# ============================================================================
# Human-Like Behavior Functions
# ============================================================================


def human_delay(min_seconds: float = None, max_seconds: float = None):
    """Add a random human-like delay."""
    if min_seconds is None:
        min_seconds = MIN_HUMAN_DELAY
    if max_seconds is None:
        max_seconds = MAX_HUMAN_DELAY
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)


def human_mouse_movement(driver: webdriver.Chrome, element):
    """Simulate human-like mouse movement to an element."""
    if not MOUSE_MOVEMENT_ENABLED:
        return

    try:
        actions = ActionChains(driver)
        # Move to a random position first (simulating mouse movement)
        viewport_width = driver.execute_script("return window.innerWidth")
        viewport_height = driver.execute_script("return window.innerHeight")

        # Random starting position
        start_x = random.randint(0, viewport_width)
        start_y = random.randint(0, viewport_height)

        # Get element location
        location = element.location
        size = element.size

        # Calculate target position (center of element)
        target_x = location["x"] + size["width"] / 2
        target_y = location["y"] + size["height"] / 2

        # Move mouse in a curved path (more human-like)
        # First move to a random intermediate point
        mid_x = (start_x + target_x) / 2 + random.randint(-50, 50)
        mid_y = (start_y + target_y) / 2 + random.randint(-50, 50)

        actions.move_by_offset(
            start_x - viewport_width / 2, start_y - viewport_height / 2
        )
        actions.pause(random.uniform(0.1, 0.3))
        actions.move_by_offset(mid_x - start_x, mid_y - start_y)
        actions.pause(random.uniform(0.1, 0.2))
        actions.move_to_element(element)
        actions.pause(random.uniform(0.1, 0.3))
        actions.perform()

        human_delay(0.1, 0.3)
    except Exception:
        # If mouse movement fails, just continue
        pass


# ============================================================================
# WebDriver Setup
# ============================================================================


def setup_driver(headless: bool = False) -> webdriver.Chrome:
    """Set up Chrome WebDriver using undetected-chromedriver for better anti-detection."""
    options = uc.ChromeOptions()

    # Download preferences
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    # Browser options
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")

    # Additional anti-detection arguments
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--disable-site-isolation-trials")

    # Use undetected-chromedriver for better anti-detection
    # Specify version_main=144 to match Chrome 144 installation
    # undetected-chromedriver will patch the driver to work with Chrome 144
    print("🔧 Setting up ChromeDriver for Chrome 144...")
    try:
        driver = uc.Chrome(
            options=options,
            version_main=144,
            use_subprocess=True,
        )
    except Exception as e:
        print(f"⚠️  Error with version_main=144: {e}")
        print("Trying without version_main (auto-detect)...")
        # Fallback: let it auto-detect and patch
        driver = uc.Chrome(options=options, use_subprocess=True)

    # Set realistic viewport
    driver.set_window_size(1920, 1080)

    return driver


# ============================================================================
# Element Interaction Helpers
# ============================================================================


def wait_for_element(
    driver: webdriver.Chrome,
    by: By,
    value: str,
    timeout: int = ELEMENT_WAIT_TIMEOUT,
):
    """Wait for an element to be present and return it."""
    wait = WebDriverWait(driver, timeout)
    return wait.until(EC.presence_of_element_located((by, value)))


def wait_for_clickable(
    driver: webdriver.Chrome,
    by: By,
    value: str,
    timeout: int = ELEMENT_WAIT_TIMEOUT,
):
    """Wait for an element to be clickable and return it."""
    wait = WebDriverWait(driver, timeout)
    return wait.until(EC.element_to_be_clickable((by, value)))


def safe_click(
    driver: webdriver.Chrome, element, retries: int = 3, scroll: bool = False
):
    """Safely click an element with retries and human-like behavior.

    Args:
        driver: WebDriver instance
        element: Element to click
        retries: Number of retry attempts
        scroll: Whether to scroll element into view (default: False to prevent unwanted scrolling)
    """
    for attempt in range(retries):
        try:
            # Human-like mouse movement before clicking
            human_mouse_movement(driver, element)

            # Only scroll if explicitly requested
            if scroll:
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                    element,
                )
                human_delay(0.3, 0.6)

            # Small delay before clicking (human-like)
            human_delay(0.1, 0.3)

            # Try regular click first
            element.click()

            # Small delay after clicking (human-like)
            human_delay(0.1, 0.2)
            return True
        except (
            StaleElementReferenceException,
            ElementNotInteractableException,
        ) as e:
            if attempt < retries - 1:
                human_delay(0.5, 1.0)
                continue
            else:
                # Try JavaScript click as fallback (this doesn't scroll)
                try:
                    human_delay(0.2, 0.4)
                    driver.execute_script("arguments[0].click();", element)
                    human_delay(0.1, 0.2)
                    return True
                except Exception:
                    raise e
    return False


# ============================================================================
# Captcha Handling
# ============================================================================


def detect_captcha(driver: webdriver.Chrome) -> bool:
    """Detect if a captcha/puzzle is present on the page."""
    captcha_indicators = [
        "captcha",
        "puzzle",
        "challenge",
        "verificação",
        "verificacao",
        "recaptcha",
        "hcaptcha",
    ]

    page_source_lower = driver.page_source.lower()
    for indicator in captcha_indicators:
        if indicator in page_source_lower:
            return True

    # Check for common captcha iframe patterns
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            src = iframe.get_attribute("src") or ""
            if any(
                ind in src.lower()
                for ind in ["captcha", "recaptcha", "hcaptcha"]
            ):
                return True
    except Exception:
        pass

    return False


def detect_security_challenge_error(driver: webdriver.Chrome) -> bool:
    """Detect if the security challenge error message is present."""
    error_messages = [
        "desafio de segurança",
        "desafio de seguranca",
        "não ficou satisfeito",
        "nao ficou satisfeito",
        "refaça o desafio",
        "refaca o desafio",
        "tente novamente em alguns minutos",
    ]

    try:
        page_source_lower = driver.page_source.lower()

        # Also check visible text on the page
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            page_text_lower = body.text.lower()
        except Exception:
            page_text_lower = ""

        for error_msg in error_messages:
            if error_msg in page_source_lower or error_msg in page_text_lower:
                return True
    except Exception:
        # If we can't check, assume no error
        pass

    return False


def wait_for_captcha_solve(
    driver: webdriver.Chrome, max_wait: int = CAPTCHA_WAIT_TIME
):
    """Wait for user to manually solve the captcha and check for security errors."""
    print("\n⚠️  CAPTCHA/PUZZLE DETECTED!")
    print("Please solve the captcha manually in the browser window.")
    print(f"Waiting up to {max_wait} seconds for you to solve it...")

    start_time = time.time()
    while time.time() - start_time < max_wait:
        # Check for security challenge error
        if detect_security_challenge_error(driver):
            print("\n❌ SECURITY CHALLENGE ERROR DETECTED!")
            print("The server rejected the captcha solution.")
            print("This may be due to automation detection.")
            print(
                "Please wait a few minutes and try again, or solve the captcha more carefully."
            )
            return False

        if not detect_captcha(driver):
            print("✅ Captcha appears to be solved. Checking for errors...")
            human_delay(2.5, 4.0)  # Give it more time to process

            # Check again for security error after solving
            if detect_security_challenge_error(driver):
                print("\n❌ SECURITY CHALLENGE ERROR DETECTED after solving!")
                print("The server rejected the captcha solution.")
                return False

            print("✅ No errors detected. Continuing...")
            return True
        human_delay(1.5, 2.5)

    print(
        "⏱️  Timeout waiting for captcha. Assuming it's solved or continuing anyway..."
    )

    # Final check for security error
    if detect_security_challenge_error(driver):
        print("\n❌ SECURITY CHALLENGE ERROR DETECTED!")
        return False

    return False


# ============================================================================
# Form Interaction Functions
# ============================================================================


def get_available_exercicios(driver: webdriver.Chrome) -> list[str]:
    """Get list of available years (Exercícios) from the dropdown."""
    try:
        # PrimeFaces: Click the trigger button to open dropdown
        try:
            trigger = driver.find_element(
                By.CSS_SELECTOR,
                "#formFinbra\\:exercicio .ui-selectonemenu-trigger",
            )
            safe_click(driver, trigger, scroll=False)
        except Exception:
            # Fallback: click container
            try:
                container = driver.find_element(
                    By.CSS_SELECTOR, "#formFinbra\\:exercicio"
                )
                safe_click(driver, container, scroll=False)
            except Exception:
                pass

        # Wait for panel to appear and be visible
        try:
            wait = WebDriverWait(driver, 10)
            panel = wait.until(
                EC.visibility_of_element_located(
                    (By.ID, "formFinbra:exercicio_panel")
                )
            )
        except TimeoutException:
            # Fallback: return a range of years
            print(
                "⚠️  Could not read exercícios from dropdown, using default range"
            )
            return [str(year) for year in range(START_YEAR, END_YEAR + 1)]

        # Additional small wait to ensure options are loaded
        human_delay(0.8, 1.5)

        # Find options in the panel
        try:
            options = panel.find_elements(
                By.CSS_SELECTOR,
                ".ui-selectonemenu-item, li.ui-selectonemenu-item",
            )
            values = []
            for opt in options:
                # Use the full displayed text - this is what we'll match against later
                value = opt.text.strip()
                if not value:
                    # Fallback to attributes if text is empty
                    value = (
                        opt.get_attribute("data-label")
                        or opt.get_attribute("data-value")
                        or ""
                    ).strip()
                if value:
                    values.append(value)
            if values:
                print(f"  Retrieved {len(values)} tabela values from dropdown")
                return values
        except Exception:
            pass

        # Fallback: return a range of years
        print(
            "⚠️  Could not read exercícios from dropdown, using default range"
        )
        return [str(year) for year in range(START_YEAR, END_YEAR + 1)]

    except Exception as e:
        print(f"Error getting exercícios: {e}")
        # Fallback: return a range of years
        return [str(year) for year in range(START_YEAR, END_YEAR + 1)]


def get_available_tabelas(driver: webdriver.Chrome) -> list[str]:
    """Get list of available tables (Tabelas) from the dropdown."""
    try:
        # PrimeFaces: Click the trigger button to open dropdown
        try:
            trigger = driver.find_element(
                By.CSS_SELECTOR,
                "#formFinbra\\:tabela .ui-selectonemenu-trigger",
            )
            safe_click(driver, trigger, scroll=False)
        except Exception:
            # Fallback: click container
            try:
                container = driver.find_element(
                    By.CSS_SELECTOR, "#formFinbra\\:tabela"
                )
                safe_click(driver, container, scroll=False)
            except Exception:
                pass

        # Wait for panel to appear and be visible
        try:
            wait = WebDriverWait(driver, 10)
            panel = wait.until(
                EC.visibility_of_element_located(
                    (By.ID, "formFinbra:tabela_panel")
                )
            )
        except TimeoutException:
            print("⚠️  Could not read tabelas from dropdown")
            return []

        # Additional small wait to ensure options are loaded
        human_delay(0.8, 1.5)

        # Find options in the panel
        try:
            options = panel.find_elements(
                By.CSS_SELECTOR,
                ".ui-selectonemenu-item, li.ui-selectonemenu-item",
            )
            values = []
            for opt in options:
                # Get the full text - this is what we'll use for matching
                value = opt.text.strip()
                if not value:
                    # Fallback to attributes
                    value = (
                        opt.get_attribute("data-label")
                        or opt.get_attribute("data-value")
                        or ""
                    ).strip()
                if value:
                    values.append(value)
            if values:
                print(f"  Found {len(values)} tabela options")
                return values
        except Exception as e:
            print(f"  Error reading options: {e}")

        # Return empty list if can't find
        print("⚠️  Could not read tabelas from dropdown")
        return []

    except Exception as e:
        print(f"Error getting tabelas: {e}")
        # Return empty list - we'll need to inspect the page manually
        return []


def set_escopo_municipios(driver: webdriver.Chrome):
    """Set Escopo to 'Municípios'."""
    try:
        print("  Selecting Escopo: Municípios")

        # PrimeFaces: Click the trigger button to open dropdown
        print("  Clicking trigger to open Escopo dropdown...")
        try:
            trigger = driver.find_element(
                By.CSS_SELECTOR,
                "#formFinbra\\:escopo .ui-selectonemenu-trigger",
            )
            safe_click(driver, trigger, scroll=False)
            human_delay(0.8, 1.5)  # Wait for dropdown to start opening
        except Exception:
            # Fallback: click container
            try:
                print("  Trigger not found, trying container...")
                container = driver.find_element(
                    By.CSS_SELECTOR, "#formFinbra\\:escopo"
                )
                safe_click(driver, container, scroll=False)
                human_delay(0.8, 1.5)  # Wait for dropdown to start opening
            except Exception as e:
                print(f"  ❌ Could not open escopo dropdown: {e}")
                return False

        # Wait for panel to appear and be visible
        print("  Waiting for dropdown panel to appear...")
        try:
            wait = WebDriverWait(driver, 10)
            panel = wait.until(
                EC.visibility_of_element_located(
                    (By.ID, "formFinbra:escopo_panel")
                )
            )
            print("  ✅ Dropdown panel appeared")
        except TimeoutException:
            print("  ❌ Panel did not appear within timeout")
            return False

        # Additional wait to ensure options are fully loaded
        print("  Waiting for options to load...")
        human_delay(1.5, 2.5)

        # Find options in the panel
        try:
            options = panel.find_elements(
                By.CSS_SELECTOR,
                ".ui-selectonemenu-item, li.ui-selectonemenu-item",
            )

            print(f"  Found {len(options)} escopo options")

            # Look for Municípios option
            for opt in options:
                text = opt.text.lower()
                value = (
                    opt.get_attribute("data-label")
                    or opt.get_attribute("data-value")
                    or opt.text.strip()
                ).lower()
                if (
                    "município" in text
                    or "municipio" in text
                    or "município" in value
                    or "municipio" in value
                ):
                    print(
                        f"  ✅ Found Municípios option: '{opt.text.strip()}'"
                    )
                    print("  Clicking option...")
                    safe_click(driver, opt, scroll=False)
                    # Wait for selection to register and dropdown to close
                    print("  Waiting for selection to register...")
                    human_delay(2.5, 3.5)
                    # Verify dropdown closed
                    try:
                        wait_closed = WebDriverWait(driver, 5)
                        wait_closed.until(
                            EC.invisibility_of_element_located(
                                (By.ID, "formFinbra:escopo_panel")
                            )
                        )
                        print("  ✅ Dropdown closed, selection registered")
                    except TimeoutException:
                        print("  ⚠️  Dropdown still visible, but continuing...")
                    print("  ✅ Selected Escopo: Municípios")
                    return True

            print("  ❌ Municípios option not found")
            return False

        except Exception as e:
            print(f"  ❌ Error finding options: {e}")
            return False

    except Exception as e:
        print(f"Error setting escopo: {e}")
        return False


def select_exercicio(driver: webdriver.Chrome, exercicio: str) -> bool:
    """Select an exercício (year) from the dropdown."""
    try:
        print(f"  Selecting exercício: {exercicio}")

        # PrimeFaces: Click the trigger button to open dropdown
        print("  Clicking trigger to open Exercício dropdown...")
        try:
            trigger = driver.find_element(
                By.CSS_SELECTOR,
                "#formFinbra\\:exercicio .ui-selectonemenu-trigger",
            )
            safe_click(driver, trigger, scroll=False)
            human_delay(0.8, 1.5)  # Wait for dropdown to start opening
        except Exception:
            # Fallback: click container
            try:
                print("  Trigger not found, trying container...")
                container = driver.find_element(
                    By.CSS_SELECTOR, "#formFinbra\\:exercicio"
                )
                safe_click(driver, container, scroll=False)
                human_delay(0.8, 1.5)  # Wait for dropdown to start opening
            except Exception as e:
                print(f"  ❌ Could not open dropdown: {e}")
                return False

        # Wait for panel to appear and be visible
        print("  Waiting for dropdown panel to appear...")
        try:
            wait = WebDriverWait(driver, 10)
            panel = wait.until(
                EC.visibility_of_element_located(
                    (By.ID, "formFinbra:exercicio_panel")
                )
            )
            print("  ✅ Dropdown panel appeared")
        except TimeoutException:
            print("  ❌ Panel did not appear within timeout")
            return False

        # Additional wait to ensure options are fully loaded
        print("  Waiting for options to load...")
        human_delay(1.5, 2.5)

        # Find options in the panel
        try:
            options = panel.find_elements(
                By.CSS_SELECTOR,
                ".ui-selectonemenu-item, li.ui-selectonemenu-item",
            )

            print(f"  Found {len(options)} exercício options")

            # Debug: print available options
            if len(options) > 0:
                print("  Available exercício options:")
                for i, opt in enumerate(options[:10]):
                    val = (
                        opt.text.strip()
                        or opt.get_attribute("data-label")
                        or opt.get_attribute("data-value")
                    )
                    print(f"    [{i}] '{val}'")

            # Look for the matching option
            exercicio_clean = str(exercicio).strip()
            print(f"  Looking for exercício: '{exercicio_clean}'")

            for opt in options:
                value = (
                    opt.get_attribute("data-label")
                    or opt.get_attribute("data-value")
                    or opt.text.strip()
                )
                value_clean = value.strip() if value else ""

                if value_clean == exercicio_clean:
                    print(f"  ✅ Found match: '{value_clean}'")
                    print("  Clicking option...")
                    safe_click(driver, opt, scroll=False)
                    # Wait for selection to register and dropdown to close
                    print("  Waiting for selection to register...")
                    human_delay(2.5, 3.5)
                    # Verify dropdown closed
                    try:
                        wait_closed = WebDriverWait(driver, 5)
                        wait_closed.until(
                            EC.invisibility_of_element_located(
                                (By.ID, "formFinbra:exercicio_panel")
                            )
                        )
                        print("  ✅ Dropdown closed, selection registered")
                    except TimeoutException:
                        print("  ⚠️  Dropdown still visible, but continuing...")

                    # VERIFY that the selection was actually registered
                    print("  Verifying selection was registered...")
                    human_delay(1.5, 2.5)  # Give it more time to update

                    # Check the input field value (PrimeFaces stores selected value here)
                    try:
                        # Try multiple possible selectors for the input field
                        input_selectors = [
                            "#formFinbra\\:exercicio_input",
                            "#formFinbra\\:exercicio input",
                            "input[id*='exercicio']",
                        ]

                        selected_value = None
                        for selector in input_selectors:
                            try:
                                input_elem = driver.find_element(
                                    By.CSS_SELECTOR, selector
                                )
                                selected_value = (
                                    input_elem.get_attribute("value")
                                    or input_elem.text
                                )
                                if selected_value:
                                    break
                            except Exception:
                                continue

                        # Also try getting the label text from the dropdown container
                        if not selected_value:
                            try:
                                label_elem = driver.find_element(
                                    By.CSS_SELECTOR,
                                    "#formFinbra\\:exercicio .ui-selectonemenu-label",
                                )
                                selected_value = label_elem.text
                            except Exception:
                                pass

                        if selected_value:
                            selected_value_clean = str(selected_value).strip()
                            print(
                                f"  Current selected value: '{selected_value_clean}'"
                            )
                            print(f"  Expected value: '{exercicio_clean}'")

                            if selected_value_clean == exercicio_clean:
                                print(
                                    f"  ✅ VERIFIED: Exercício {exercicio} is correctly selected"
                                )
                                return True
                            else:
                                print(
                                    f"  ❌ VERIFICATION FAILED: Expected '{exercicio_clean}' but got '{selected_value_clean}'"
                                )
                                print("  Retrying selection...")
                                # Retry once
                                human_delay(1.5, 2.5)
                                # Try clicking the option again
                                safe_click(driver, opt, scroll=False)
                                human_delay(2.5, 3.5)
                                # Check again
                                for selector in input_selectors:
                                    try:
                                        input_elem = driver.find_element(
                                            By.CSS_SELECTOR, selector
                                        )
                                        selected_value = (
                                            input_elem.get_attribute("value")
                                            or input_elem.text
                                        )
                                        if selected_value:
                                            break
                                    except Exception:
                                        continue
                                if not selected_value:
                                    try:
                                        label_elem = driver.find_element(
                                            By.CSS_SELECTOR,
                                            "#formFinbra\\:exercicio .ui-selectonemenu-label",
                                        )
                                        selected_value = label_elem.text
                                    except Exception:
                                        pass

                                if selected_value:
                                    selected_value_clean = str(
                                        selected_value
                                    ).strip()
                                    if selected_value_clean == exercicio_clean:
                                        print(
                                            f"  ✅ VERIFIED after retry: Exercício {exercicio} is correctly selected"
                                        )
                                        return True
                                    else:
                                        print(
                                            f"  ❌ VERIFICATION FAILED after retry: Expected '{exercicio_clean}' but got '{selected_value_clean}'"
                                        )
                                        return False
                                else:
                                    print(
                                        "  ⚠️  Could not verify selection (value not found), but continuing..."
                                    )
                                    return True
                        else:
                            print(
                                "  ⚠️  Could not find selected value element, but continuing..."
                            )
                            return True
                    except Exception as e:
                        print(
                            f"  ⚠️  Error verifying selection: {e}, but continuing..."
                        )
                        return True

            print(
                f"  ❌ Option '{exercicio_clean}' not found in {len(options)} options"
            )
            return False

        except Exception as e:
            print(f"  ❌ Error finding options: {e}")
            import traceback

            traceback.print_exc()
            return False

    except Exception as e:
        print(f"Error selecting exercício {exercicio}: {e}")
        import traceback

        traceback.print_exc()
        return False


def select_tabela(driver: webdriver.Chrome, tabela: str) -> bool:
    """Select a tabela from the dropdown."""
    try:
        print(f"  Selecting tabela: {tabela}")

        # PrimeFaces: Click the trigger button to open dropdown
        print("  Clicking trigger to open Tabela dropdown...")
        try:
            trigger = driver.find_element(
                By.CSS_SELECTOR,
                "#formFinbra\\:tabela .ui-selectonemenu-trigger",
            )
            safe_click(driver, trigger, scroll=False)
            human_delay(0.8, 1.5)  # Wait for dropdown to start opening
            print("  Clicked trigger button")
        except Exception as e:
            # Fallback: click container
            try:
                print(f"  Trigger not found ({e}), trying container...")
                container = driver.find_element(
                    By.CSS_SELECTOR, "#formFinbra\\:tabela"
                )
                safe_click(driver, container, scroll=False)
                human_delay(0.8, 1.5)  # Wait for dropdown to start opening
                print("  Clicked container")
            except Exception as e2:
                print(f"  ❌ Could not open tabela dropdown: {e2}")
                return False

        # Wait for panel to appear and be visible
        print("  Waiting for dropdown panel to appear...")
        try:
            wait = WebDriverWait(driver, 10)
            panel = wait.until(
                EC.visibility_of_element_located(
                    (By.ID, "formFinbra:tabela_panel")
                )
            )
            print("  ✅ Dropdown panel appeared")
        except TimeoutException:
            print("  ❌ Panel did not appear within timeout")
            return False

        # Additional wait to ensure options are fully loaded
        print("  Waiting for options to load...")
        human_delay(1.5, 2.5)

        # Find options in the panel
        try:
            options = panel.find_elements(
                By.CSS_SELECTOR,
                ".ui-selectonemenu-item, li.ui-selectonemenu-item",
            )

            print(f"  Found {len(options)} tabela options")

            # Debug: print all available options and what we're looking for
            tabela_clean = str(tabela).strip()
            print(f"  Looking for tabela: '{tabela_clean}'")

            if len(options) > 0:
                print("  Available tabela options:")
                for i, opt in enumerate(options):
                    opt_text = opt.text.strip()
                    print(f"    [{i}] '{opt_text}'")

            # Look for the matching option - use full text for matching
            for opt in options:
                # Get the full displayed text - this is what we match against
                value = opt.text.strip()
                if not value:
                    # Fallback to attributes if text is empty
                    value = (
                        opt.get_attribute("data-label")
                        or opt.get_attribute("data-value")
                        or ""
                    ).strip()

                value_clean = value.strip() if value else ""

                # Try exact match first
                if value_clean == tabela_clean:
                    print(f"  ✅ Found exact match: '{value_clean}'")
                    print("  Clicking option...")
                    safe_click(driver, opt, scroll=False)
                    # Wait for selection to register and dropdown to close
                    print("  Waiting for selection to register...")
                    human_delay(2.5, 3.5)
                    # Verify dropdown closed
                    try:
                        wait_closed = WebDriverWait(driver, 5)
                        wait_closed.until(
                            EC.invisibility_of_element_located(
                                (By.ID, "formFinbra:tabela_panel")
                            )
                        )
                        print("  ✅ Dropdown closed, selection registered")
                    except TimeoutException:
                        print("  ⚠️  Dropdown still visible, but continuing...")
                    print(f"  ✅ Selected tabela: '{value_clean}'")
                    return True

                # Try case-insensitive match
                if value_clean.lower() == tabela_clean.lower():
                    print(
                        f"  ✅ Found case-insensitive match: '{value_clean}'"
                    )
                    print("  Clicking option...")
                    safe_click(driver, opt, scroll=False)
                    # Wait for selection to register and dropdown to close
                    print("  Waiting for selection to register...")
                    human_delay(2.5, 3.5)
                    # Verify dropdown closed
                    try:
                        wait_closed = WebDriverWait(driver, 5)
                        wait_closed.until(
                            EC.invisibility_of_element_located(
                                (By.ID, "formFinbra:tabela_panel")
                            )
                        )
                        print("  ✅ Dropdown closed, selection registered")
                    except TimeoutException:
                        print("  ⚠️  Dropdown still visible, but continuing...")
                    print(f"  ✅ Selected tabela: '{value_clean}'")
                    return True

                # Try partial match (tabela value contained in option text)
                if tabela_clean.lower() in value_clean.lower():
                    print(f"  ✅ Found partial match: '{value_clean}'")
                    print("  Clicking option...")
                    safe_click(driver, opt, scroll=False)
                    # Wait for selection to register and dropdown to close
                    print("  Waiting for selection to register...")
                    human_delay(2.5, 3.5)
                    # Verify dropdown closed
                    try:
                        wait_closed = WebDriverWait(driver, 5)
                        wait_closed.until(
                            EC.invisibility_of_element_located(
                                (By.ID, "formFinbra:tabela_panel")
                            )
                        )
                        print("  ✅ Dropdown closed, selection registered")
                    except TimeoutException:
                        print("  ⚠️  Dropdown still visible, but continuing...")
                    print(f"  ✅ Selected tabela: '{value_clean}'")
                    return True

            print(f"  ❌ Option '{tabela_clean}' not found in dropdown")
            print(f"  Searched in {len(options)} options")
            return False

        except Exception as e:
            print(f"  ❌ Error finding options: {e}")
            import traceback

            traceback.print_exc()
            return False

    except Exception as e:
        print(f"Error selecting tabela {tabela}: {e}")
        return False


def click_consultar(driver: webdriver.Chrome) -> bool:
    """Click the 'Consultar' button."""
    try:
        # Known ID from page inspection
        selectors = [
            "formFinbra:captcha:j_idt246",  # Known ID
            "button[type='submit']",
            "input[type='submit']",
            "input[value*='Consultar']",
        ]

        # Also try by text content
        xpaths = [
            "//button[contains(text(), 'Consultar')]",
            "//input[@value='Consultar' or contains(@value, 'Consultar')]",
            "//a[contains(text(), 'Consultar')]",
            "//*[contains(text(), 'Consultar') and (self::button or self::input or self::a)]",
        ]

        for selector in selectors:
            try:
                if ":" in selector:
                    button = driver.find_element(By.ID, selector)
                else:
                    button = driver.find_element(By.CSS_SELECTOR, selector)
                if (
                    "consultar" in button.text.lower()
                    or "consultar"
                    in (button.get_attribute("value") or "").lower()
                    or ":" in selector  # Known ID, just click it
                ):
                    safe_click(driver, button, scroll=False)
                    human_delay(ACTION_DELAY - 0.5, ACTION_DELAY + 0.5)
                    return True
            except Exception:
                continue

        for xpath in xpaths:
            try:
                button = driver.find_element(By.XPATH, xpath)
                safe_click(driver, button, scroll=False)
                time.sleep(ACTION_DELAY)
                return True
            except Exception:
                continue

        return False
    except Exception as e:
        print(f"Error clicking Consultar: {e}")
        return False


# ============================================================================
# Download Handling Functions
# ============================================================================


def wait_for_download_complete(
    driver: webdriver.Chrome, timeout: int = 120
) -> bool:
    """Wait for download to complete by checking download directory."""
    start_time = time.time()
    initial_files = set(DOWNLOAD_DIR.glob("*"))

    while time.time() - start_time < timeout:
        current_files = set(DOWNLOAD_DIR.glob("*"))
        new_files = current_files - initial_files

        # Check if any new files appeared (excluding progress file)
        new_files = {
            f for f in new_files if f.name != "download_progress.json"
        }

        if new_files:
            # Wait a bit more to ensure download is complete
            human_delay(2.5, 3.5)
            return True

        human_delay(0.8, 1.5)

    return False


def get_downloaded_file(exercicio: str, tabela: str) -> Path | None:
    """Get the most recently downloaded file."""
    files = [
        f for f in DOWNLOAD_DIR.glob("*") if f.name != "download_progress.json"
    ]
    if files:
        # Return most recently modified file
        return max(files, key=lambda f: f.stat().st_mtime)
    return None


def rename_downloaded_file(exercicio: str, tabela: str, file_path: Path):
    """Rename downloaded file with a meaningful name."""
    try:
        suffix = file_path.suffix
        new_name = f"exercicio_{exercicio}_tabela_{tabela}{suffix}"
        new_path = DOWNLOAD_DIR / new_name
        file_path.rename(new_path)
        return new_path
    except Exception as e:
        print(f"Error renaming file: {e}")
        return file_path


def download_file(
    driver: webdriver.Chrome, exercicio: str, tabela: str | None = None
) -> bool:
    """Download a file for given exercício and tabela."""
    tabela_key = tabela or "default"
    print(f"\n📥 Downloading: Exercício={exercicio}, Tabela={tabela_key}")

    # Check if already downloaded
    if is_downloaded(exercicio, tabela_key):
        print("⏭️  Already downloaded, skipping...")
        return True

    try:
        # Navigate back to form if needed (in case we're on a results page)
        current_url = driver.current_url
        if "finbra_list" not in current_url:
            driver.get(URL)
            human_delay(2.5, 3.5)

        # IMPORTANT: Select fields in order with waits between each
        # The page updates after each selection, so we must wait

        # Step 1: Select Exercício first
        print("\n" + "=" * 60)
        print("  Step 1: Selecting Exercício...")
        print("=" * 60)
        if not select_exercicio(driver, exercicio):
            raise Exception(f"Failed to select exercício {exercicio}")
        # Wait for page to fully update after exercício selection
        print("  Waiting for page to update after Exercício selection...")
        human_delay(3.5, 5.0)

        # Step 2: Select Escopo (required field - always set to Municípios)
        print("\n" + "=" * 60)
        print("  Step 2: Selecting Escopo (Municípios)...")
        print("=" * 60)
        if not set_escopo_municipios(driver):
            raise Exception("Failed to select Escopo (Municípios)")
        # Wait for page to fully update after escopo selection
        print("  Waiting for page to update after Escopo selection...")
        human_delay(3.5, 5.0)

        # Step 3: Select Tabela (required field)
        if not tabela:
            raise Exception("Tabela is required but not provided")
        print("\n" + "=" * 60)
        print("  Step 3: Selecting Tabela...")
        print("=" * 60)
        if not select_tabela(driver, tabela):
            raise Exception(f"Failed to select tabela {tabela}")
        # Wait for page to fully update after tabela selection
        print("  Waiting for page to update after Tabela selection...")
        human_delay(3.5, 5.0)

        # Step 4: Click Consultar
        print("\n" + "=" * 60)
        print("  Step 4: Clicking Consultar...")
        print("=" * 60)
        if not click_consultar(driver):
            raise Exception("Failed to click Consultar button")

        # Check for captcha
        time.sleep(3)  # Wait for page to load

        # Check for security challenge error first
        if detect_security_challenge_error(driver):
            print("\n❌ SECURITY CHALLENGE ERROR DETECTED!")
            print(
                "The server is blocking the request due to automation detection."
            )
            raise Exception("Security challenge error - server blocking")

        if detect_captcha(driver):
            captcha_solved = wait_for_captcha_solve(driver)
            if not captcha_solved:
                raise Exception(
                    "Captcha solving failed or security error occurred"
                )

            # Check again for security error after solving
            human_delay(1.5, 2.5)
            if detect_security_challenge_error(driver):
                raise Exception("Security challenge error after captcha")

        # Wait for download link or button to appear
        # Look for download button/link
        download_selectors = [
            "a[href*='download']",
            "button[onclick*='download']",
        ]

        download_xpaths = [
            "//a[contains(text(), 'Download') or contains(text(), 'Baixar')]",
            "//button[contains(text(), 'Download') or contains(text(), 'Baixar')]",
            "//a[contains(@href, 'download')]",
        ]

        download_element = None
        for selector in download_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    text = elem.text.lower()
                    if "download" in text or "baixar" in text:
                        download_element = elem
                        break
                if download_element:
                    break
            except Exception:
                continue

        if download_element is None:
            for xpath in download_xpaths:
                try:
                    download_element = driver.find_element(By.XPATH, xpath)
                    break
                except Exception:
                    continue

        if download_element:
            safe_click(driver, download_element, scroll=False)
            if wait_for_download_complete(driver):
                downloaded_file = get_downloaded_file(exercicio, tabela_key)
                if downloaded_file:
                    rename_downloaded_file(
                        exercicio, tabela_key, downloaded_file
                    )
                    mark_completed(exercicio, tabela_key)
                    print(
                        f"✅ Successfully downloaded: {downloaded_file.name}"
                    )
                    return True
        else:
            # Maybe the page automatically triggers download, or we need to look for a different pattern
            print(
                "⚠️  Could not find download button. Waiting to see if download starts automatically..."
            )
            if wait_for_download_complete(driver, timeout=30):
                downloaded_file = get_downloaded_file(exercicio, tabela_key)
                if downloaded_file:
                    rename_downloaded_file(
                        exercicio, tabela_key, downloaded_file
                    )
                    mark_completed(exercicio, tabela_key)
                    print(
                        f"✅ Successfully downloaded: {downloaded_file.name}"
                    )
                    return True

        raise Exception("Download did not complete or file not found")

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error downloading {exercicio}/{tabela_key}: {error_msg}")
        mark_failed(exercicio, tabela_key, error_msg)
        return False


# ============================================================================
# Main Functions
# ============================================================================


def download_all_files(
    headless: bool = False,
    tabelas_manual: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
):
    """Main function to download all files.

    Args:
        headless: Whether to run browser in headless mode
        tabelas_manual: Optional list of tabela values if automatic detection fails
        start_year: Start year for downloads (defaults to START_YEAR constant)
        end_year: End year for downloads (defaults to END_YEAR constant)
    """
    # Use provided years or fall back to constants
    start_year = start_year if start_year is not None else START_YEAR
    end_year = end_year if end_year is not None else END_YEAR

    driver = None
    try:
        print("🚀 Starting SICONFI download process...")
        print(f"📁 Download directory: {DOWNLOAD_DIR.absolute()}")

        # Setup driver
        driver = setup_driver(headless=headless)
        driver.get(URL)
        print(f"🌐 Opened URL: {URL}")

        # Wait for page to load
        human_delay(4.0, 6.0)

        # Get available exercícios first
        print("\n📋 Getting available exercícios...")
        exercicios = get_available_exercicios(driver)

        if not exercicios:
            print(
                "⚠️  Could not get exercícios automatically. Using default range."
            )
            exercicios = [
                str(year) for year in range(start_year, end_year + 1)
            ]

        # Sort exercícios numerically to ensure 2013 comes first, then 2014, etc.
        try:
            # Convert to int for sorting, then back to string
            exercicios = sorted(
                exercicios, key=lambda x: int(x) if x.isdigit() else 9999
            )
            print(f"📊 Sorted exercícios: {exercicios[:10]}...")
        except Exception:
            # If sorting fails, just use as-is
            print(
                f"📊 Found {len(exercicios)} exercícios: {exercicios[:5]}..."
            )

        print(
            f"📊 Processing {len(exercicios)} exercícios starting with: {exercicios[0] if exercicios else 'N/A'}"
        )

        # IMPORTANT: Tabelas only appear AFTER Exercício and Escopo are selected
        # So we need to select them first, then get tabelas dynamically for each exercício

        total_downloads = 0
        current_download = 0

        # Process each exercício
        for exercicio in exercicios:
            print(f"\n{'#' * 70}")
            print(f"# Processing Exercício: {exercicio}")
            print(f"{'#' * 70}")

            # Navigate to form (fresh start for each exercício)
            driver.get(URL)
            human_delay(2.5, 4.0)

            # Step 1: Select Exercício
            print("\n" + "=" * 60)
            print("  Step 1: Selecting Exercício...")
            print("=" * 60)

            # Retry exercício selection until verification passes
            exercicio_selected = False
            for attempt in range(3):  # Try up to 3 times
                if select_exercicio(driver, exercicio):
                    # The function now includes verification, so if it returns True, we're good
                    exercicio_selected = True
                    break
                else:
                    print(f"  ⚠️  Attempt {attempt + 1} failed, retrying...")
                    human_delay(1.5, 3.0)
                    # Refresh page if needed
                    if attempt < 2:
                        driver.get(URL)
                        human_delay(2.5, 4.0)

            if not exercicio_selected:
                print(
                    f"  ❌ Failed to select and verify exercício {exercicio} after 3 attempts, skipping..."
                )
                continue

            # Wait for page to update after exercício selection
            print("  Waiting for page to update after Exercício selection...")
            human_delay(3.5, 5.0)

            # Step 2: Select Escopo (required field - always set to Municípios)
            print("\n" + "=" * 60)
            print("  Step 2: Selecting Escopo (Municípios)...")
            print("=" * 60)
            if not set_escopo_municipios(driver):
                print("  ❌ Failed to select Escopo (Municípios), skipping...")
                continue
            # Wait for page to update after escopo selection
            print("  Waiting for page to update after Escopo selection...")
            human_delay(3.5, 5.0)

            # Step 3: NOW get available Tabelas (they only appear after Exercício and Escopo are selected)
            if not tabelas_manual:
                print("\n" + "=" * 60)
                print(
                    "  Getting available Tabelas (after Exercício and Escopo selected)..."
                )
                print("=" * 60)
                tabelas = get_available_tabelas(driver)
                if not tabelas:
                    print(
                        "  ⚠️  Could not get tabelas. Skipping this exercício..."
                    )
                    continue
                print(
                    f"  Found {len(tabelas)} tabelas for exercício {exercicio}"
                )
            else:
                tabelas = tabelas_manual
                print(f"  Using {len(tabelas)} manually provided tabelas")

            # Update total count
            total_downloads += len(tabelas)

            # Step 4: For each tabela, select it and download
            for tabela in tabelas:
                current_download += 1
                print(f"\n{'=' * 60}")
                print(
                    f"Progress: {current_download}/{total_downloads} ({current_download / total_downloads * 100:.1f}%)"
                )
                print(f"  Exercício: {exercicio}, Tabela: {tabela}")
                print(f"{'=' * 60}")

                # Check if already downloaded
                tabela_key = tabela or "default"
                if is_downloaded(exercicio, tabela_key):
                    print("⏭️  Already downloaded, skipping...")
                    continue

                # Step 4a: Select Tabela
                print("\n" + "=" * 60)
                print("  Step 3: Selecting Tabela...")
                print("=" * 60)
                if not select_tabela(driver, tabela):
                    print(
                        f"  ❌ Failed to select tabela {tabela}, skipping..."
                    )
                    mark_failed(
                        exercicio,
                        tabela_key,
                        f"Failed to select tabela {tabela}",
                    )
                    continue
                # Wait for page to update after tabela selection
                print("  Waiting for page to update after Tabela selection...")
                human_delay(3.5, 5.0)

                # Step 4b: Click Consultar
                print("\n" + "=" * 60)
                print("  Step 4: Clicking Consultar...")
                print("=" * 60)
                if not click_consultar(driver):
                    print("  ❌ Failed to click Consultar, skipping...")
                    mark_failed(
                        exercicio, tabela_key, "Failed to click Consultar"
                    )
                    continue

                # Step 4c: Handle captcha and download
                print("\n" + "=" * 60)
                print("  Step 5: Checking for captcha and downloading...")
                print("=" * 60)

                # Check for captcha
                human_delay(2.5, 4.0)  # Wait for page to load

                # Check for security challenge error first
                if detect_security_challenge_error(driver):
                    print("\n❌ SECURITY CHALLENGE ERROR DETECTED!")
                    print(
                        "The server is blocking the request due to automation detection."
                    )
                    print(
                        f"Waiting {SECURITY_ERROR_DELAY} seconds before retrying..."
                    )
                    time.sleep(
                        SECURITY_ERROR_DELAY
                    )  # Wait longer when blocked
                    mark_failed(
                        exercicio,
                        tabela_key,
                        "Security challenge error - server blocking",
                    )
                    continue

                if detect_captcha(driver):
                    captcha_solved = wait_for_captcha_solve(driver)
                    if not captcha_solved:
                        # If captcha solving failed or security error occurred
                        print(
                            "⚠️  Captcha solving failed or security error occurred."
                        )
                        print("Waiting 30 seconds before retrying...")
                        time.sleep(30)
                        mark_failed(
                            exercicio,
                            tabela_key,
                            "Captcha solving failed or security error",
                        )
                        continue

                    # Check again for security error after solving
                    human_delay(1.5, 2.5)
                    if detect_security_challenge_error(driver):
                        print(
                            "\n❌ SECURITY CHALLENGE ERROR DETECTED after captcha!"
                        )
                        print("Waiting 60 seconds before retrying...")
                        time.sleep(60)
                        mark_failed(
                            exercicio,
                            tabela_key,
                            "Security challenge error after captcha",
                        )
                        continue

                # Wait for download link or button to appear
                download_success = False
                download_selectors = [
                    "a[href*='download']",
                    "button[onclick*='download']",
                ]

                download_xpaths = [
                    "//a[contains(text(), 'Download') or contains(text(), 'Baixar')]",
                    "//button[contains(text(), 'Download') or contains(text(), 'Baixar')]",
                    "//a[contains(@href, 'download')]",
                ]

                download_element = None
                for selector in download_selectors:
                    try:
                        elements = driver.find_elements(
                            By.CSS_SELECTOR, selector
                        )
                        for elem in elements:
                            text = elem.text.lower()
                            if "download" in text or "baixar" in text:
                                download_element = elem
                                break
                        if download_element:
                            break
                    except Exception:
                        continue

                if download_element is None:
                    for xpath in download_xpaths:
                        try:
                            download_element = driver.find_element(
                                By.XPATH, xpath
                            )
                            break
                        except Exception:
                            continue

                if download_element:
                    safe_click(driver, download_element, scroll=False)
                    if wait_for_download_complete(driver):
                        downloaded_file = get_downloaded_file(
                            exercicio, tabela_key
                        )
                        if downloaded_file:
                            rename_downloaded_file(
                                exercicio, tabela_key, downloaded_file
                            )
                            mark_completed(exercicio, tabela_key)
                            print(
                                f"✅ Successfully downloaded: {downloaded_file.name}"
                            )
                            download_success = True
                else:
                    # Maybe the page automatically triggers download
                    print(
                        "⚠️  Could not find download button. Waiting to see if download starts automatically..."
                    )
                    if wait_for_download_complete(driver, timeout=30):
                        downloaded_file = get_downloaded_file(
                            exercicio, tabela_key
                        )
                        if downloaded_file:
                            rename_downloaded_file(
                                exercicio, tabela_key, downloaded_file
                            )
                            mark_completed(exercicio, tabela_key)
                            print(
                                f"✅ Successfully downloaded: {downloaded_file.name}"
                            )
                            download_success = True

                if not download_success:
                    print(
                        f"⚠️  Download did not complete for {exercicio}/{tabela_key}"
                    )
                    mark_failed(
                        exercicio, tabela_key, "Download did not complete"
                    )

                # Be nice to the server
                human_delay(ACTION_DELAY, ACTION_DELAY + 1.0)

                # Navigate back to form for next tabela (or next exercício will refresh)
                driver.get(URL)
                human_delay(2.5, 4.0)

        print("\n✅ Download process completed!")
        print(f"📁 Files saved to: {DOWNLOAD_DIR.absolute()}")

    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        traceback.print_exc()
    finally:
        if driver:
            print("\n🔒 Closing browser...")
            driver.quit()


def inspect_page_structure():
    """Open the page and print useful information about its structure."""
    driver = setup_driver(headless=False)
    try:
        driver.get(URL)
        print("Page loaded. Inspecting structure...\n")
        time.sleep(5)

        # Find all select elements
        selects = driver.find_elements(By.TAG_NAME, "select")
        print(f"Found {len(selects)} select elements:")
        for i, select in enumerate(selects):
            select_id = select.get_attribute("id")
            select_name = select.get_attribute("name")
            select_class = select.get_attribute("class")
            print(
                f"  Select {i + 1}: id='{select_id}', name='{select_name}', class='{select_class}'"
            )
            try:
                options = Select(select).options
                print(
                    f"    Options: {[opt.text for opt in options[:5]]}... ({len(options)} total)"
                )
            except Exception:
                pass

        # Find all buttons
        buttons = driver.find_elements(By.TAG_NAME, "button")
        inputs = driver.find_elements(
            By.CSS_SELECTOR, "input[type='submit'], input[type='button']"
        )
        print(
            f"\nFound {len(buttons)} buttons and {len(inputs)} input buttons:"
        )
        for btn in buttons[:10]:
            print(
                f"  Button: text='{btn.text}', id='{btn.get_attribute('id')}', class='{btn.get_attribute('class')}'"
            )
        for inp in inputs[:10]:
            print(
                f"  Input: value='{inp.get_attribute('value')}', id='{inp.get_attribute('id')}', class='{inp.get_attribute('class')}'"
            )

        # Find radio buttons
        radios = driver.find_elements(By.CSS_SELECTOR, "input[type='radio']")
        print(f"\nFound {len(radios)} radio buttons:")
        for radio in radios[:10]:
            print(
                f"  Radio: value='{radio.get_attribute('value')}', id='{radio.get_attribute('id')}', name='{radio.get_attribute('name')}'"
            )

        print("\n\nBrowser will stay open for manual inspection.")
        input("Press Enter to close browser...")

    finally:
        driver.quit()


# ============================================================================
# Command Line Interface
# ============================================================================


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Download raw data from SICONFI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with visible browser (recommended for captcha handling)
  python download_raw_data.py

  # Run in headless mode
  python download_raw_data.py --headless

  # Inspect page structure
  python download_raw_data.py --inspect

  # Specify tabelas manually
  python download_raw_data.py --tabelas tabela1 tabela2 tabela3
        """,
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode (not recommended if captchas are frequent)",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Inspect page structure and exit",
    )
    parser.add_argument(
        "--tabelas",
        nargs="+",
        help="Manually specify tabela values (space-separated)",
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

    args = parser.parse_args()

    if args.inspect:
        inspect_page_structure()
        return

    download_all_files(
        headless=args.headless,
        tabelas_manual=args.tabelas,
        start_year=args.start_year,
        end_year=args.end_year,
    )


if __name__ == "__main__":
    main()
