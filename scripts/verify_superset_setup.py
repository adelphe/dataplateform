#!/usr/bin/env python3
"""
Superset Setup Verification Script

Verifies that Superset is properly configured with:
- Database connection to PostgreSQL warehouse
- Datasets for dbt mart models
- Pre-configured dashboards
- RBAC roles

Exit codes:
  0 - All checks passed
  1 - One or more checks failed
"""

import sys
import time
from typing import Tuple

import requests

# Configuration
SUPERSET_URL = "http://localhost:8088"
SUPERSET_USER = "admin"
SUPERSET_PASSWORD = "admin"
MAX_RETRIES = 5
RETRY_DELAY = 2

# Expected resources
EXPECTED_DATABASES = ["postgres_warehouse"]
EXPECTED_DATASETS = [
    "mart_customer_analytics",
    "mart_product_analytics",
    "mart_daily_summary",
]
EXPECTED_DASHBOARDS = [
    "Customer Insights",
    "Product Performance",
    "Daily Operations",
]
EXPECTED_ROLES = ["Admin", "Analyst", "Viewer"]


class SupersetVerifier:
    """Verifies Superset setup and configuration."""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.access_token = None
        self.results = []

    def log_result(self, check: str, passed: bool, message: str = ""):
        """Log a verification result."""
        status = "✓ PASS" if passed else "✗ FAIL"
        full_message = f"{status}: {check}"
        if message:
            full_message += f" - {message}"
        print(full_message)
        self.results.append({"check": check, "passed": passed, "message": message})

    def check_health(self) -> bool:
        """Check if Superset is accessible."""
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(f"{self.base_url}/health", timeout=10)
                if response.status_code == 200:
                    self.log_result("Superset Health Check", True, "Service is healthy")
                    return True
            except requests.RequestException:
                pass

            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

        self.log_result("Superset Health Check", False, "Service not reachable")
        return False

    def authenticate(self) -> bool:
        """Authenticate with Superset API."""
        try:
            # Get CSRF token first
            csrf_response = self.session.get(
                f"{self.base_url}/api/v1/security/csrf_token/",
                timeout=10,
            )

            # Login
            login_payload = {
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            }

            response = self.session.post(
                f"{self.base_url}/api/v1/security/login",
                json=login_payload,
                timeout=10,
            )

            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get("access_token")
                self.session.headers.update(
                    {"Authorization": f"Bearer {self.access_token}"}
                )
                self.log_result("Authentication", True, f"Logged in as {self.username}")
                return True
            else:
                self.log_result(
                    "Authentication", False, f"Login failed: {response.status_code}"
                )
                return False

        except requests.RequestException as e:
            self.log_result("Authentication", False, str(e))
            return False

    def check_databases(self) -> bool:
        """Verify database connections exist."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/database/",
                timeout=10,
            )

            if response.status_code != 200:
                self.log_result(
                    "Database Connections", False, f"API error: {response.status_code}"
                )
                return False

            data = response.json()
            databases = [db["database_name"] for db in data.get("result", [])]

            all_found = True
            for expected_db in EXPECTED_DATABASES:
                if expected_db in databases:
                    self.log_result(
                        f"Database: {expected_db}", True, "Connection exists"
                    )
                else:
                    self.log_result(
                        f"Database: {expected_db}", False, "Connection not found"
                    )
                    all_found = False

            return all_found

        except requests.RequestException as e:
            self.log_result("Database Connections", False, str(e))
            return False

    def check_datasets(self) -> bool:
        """Verify datasets for dbt marts exist."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/dataset/",
                params={"q": "(page_size:100)"},
                timeout=10,
            )

            if response.status_code != 200:
                self.log_result(
                    "Datasets", False, f"API error: {response.status_code}"
                )
                return False

            data = response.json()
            datasets = [ds["table_name"] for ds in data.get("result", [])]

            all_found = True
            for expected_ds in EXPECTED_DATASETS:
                if expected_ds in datasets:
                    self.log_result(f"Dataset: {expected_ds}", True, "Found")
                else:
                    self.log_result(f"Dataset: {expected_ds}", False, "Not found")
                    all_found = False

            return all_found

        except requests.RequestException as e:
            self.log_result("Datasets", False, str(e))
            return False

    def check_dashboards(self) -> bool:
        """Verify pre-configured dashboards exist."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/dashboard/",
                params={"q": "(page_size:100)"},
                timeout=10,
            )

            if response.status_code != 200:
                self.log_result(
                    "Dashboards", False, f"API error: {response.status_code}"
                )
                return False

            data = response.json()
            dashboards = [db["dashboard_title"] for db in data.get("result", [])]

            all_found = True
            for expected_dash in EXPECTED_DASHBOARDS:
                if expected_dash in dashboards:
                    self.log_result(f"Dashboard: {expected_dash}", True, "Found")
                else:
                    self.log_result(f"Dashboard: {expected_dash}", False, "Not found")
                    all_found = False

            return all_found

        except requests.RequestException as e:
            self.log_result("Dashboards", False, str(e))
            return False

    def check_roles(self) -> bool:
        """Verify RBAC roles exist."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/security/roles/",
                params={"q": "(page_size:100)"},
                timeout=10,
            )

            if response.status_code != 200:
                self.log_result("Roles", False, f"API error: {response.status_code}")
                return False

            data = response.json()
            roles = [role["name"] for role in data.get("result", [])]

            all_found = True
            for expected_role in EXPECTED_ROLES:
                if expected_role in roles:
                    self.log_result(f"Role: {expected_role}", True, "Found")
                else:
                    self.log_result(f"Role: {expected_role}", False, "Not found")
                    all_found = False

            return all_found

        except requests.RequestException as e:
            self.log_result("Roles", False, str(e))
            return False

    def run_all_checks(self) -> Tuple[int, int]:
        """Run all verification checks."""
        print("=" * 60)
        print("Superset Setup Verification")
        print("=" * 60)
        print()

        # Health check first
        if not self.check_health():
            print()
            print("Superset is not accessible. Ensure the service is running:")
            print("  make start")
            print("  make superset-logs")
            return (0, 1)

        print()

        # Authenticate
        if not self.authenticate():
            print()
            print("Authentication failed. Check credentials.")
            return (1, 1)

        print()

        # Run all checks
        self.check_databases()
        print()
        self.check_datasets()
        print()
        self.check_dashboards()
        print()
        self.check_roles()

        # Summary
        passed = sum(1 for r in self.results if r["passed"])
        failed = sum(1 for r in self.results if not r["passed"])

        print()
        print("=" * 60)
        print(f"Summary: {passed} passed, {failed} failed")
        print("=" * 60)

        return (passed, failed)


def main():
    """Main entry point."""
    verifier = SupersetVerifier(
        base_url=SUPERSET_URL,
        username=SUPERSET_USER,
        password=SUPERSET_PASSWORD,
    )

    passed, failed = verifier.run_all_checks()

    if failed > 0:
        print()
        print("Some checks failed. To troubleshoot:")
        print("  1. Check Superset logs: make superset-logs")
        print("  2. Re-run initialization: make superset-init")
        print("  3. See docs/superset-guide.md for more help")
        sys.exit(1)
    else:
        print()
        print("All checks passed! Superset is properly configured.")
        print(f"Access at: {SUPERSET_URL}")
        sys.exit(0)


if __name__ == "__main__":
    main()
