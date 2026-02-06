"""
OpenMetadata RBAC Initialization Script

This script initializes roles, policies, teams, and classifications
in OpenMetadata based on the roles_config.yaml configuration.

Usage:
    python init_rbac.py --config roles_config.yaml --server http://localhost:8585
"""

import argparse
import logging
import os
import sys
from typing import Any

import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OpenMetadataRBACInitializer:
    """Initialize RBAC configuration in OpenMetadata."""

    def __init__(
        self,
        server_url: str,
        jwt_token: str | None = None,
        username: str | None = None,
        password: str | None = None
    ):
        self.server_url = server_url.rstrip("/")
        self.api_base = f"{self.server_url}/api/v1"
        self.headers = {
            "Content-Type": "application/json",
        }
        # Support JWT token authentication
        if jwt_token:
            self.headers["Authorization"] = f"Bearer {jwt_token}"
        # Support basic auth with username/password
        elif username and password:
            import base64
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            self.headers["Authorization"] = f"Basic {credentials}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None
    ) -> dict | None:
        """Make HTTP request to OpenMetadata API."""
        url = f"{self.api_base}/{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None

    def create_policy(self, policy: dict) -> bool:
        """Create a policy in OpenMetadata."""
        logger.info(f"Creating policy: {policy['name']}")

        policy_data = {
            "name": policy["name"],
            "description": policy.get("description", ""),
            "rules": []
        }

        for rule in policy.get("rules", []):
            rule_data = {
                "name": rule["name"],
                "resources": rule.get("resources", []),
                "operations": rule.get("operations", []),
                "effect": rule.get("effect", "allow"),
            }
            if "condition" in rule:
                rule_data["condition"] = rule["condition"]
            policy_data["rules"].append(rule_data)

        result = self._make_request("POST", "policies", policy_data)
        return result is not None

    def create_role(self, role: dict) -> bool:
        """Create a role in OpenMetadata."""
        logger.info(f"Creating role: {role['name']}")

        role_data = {
            "name": role["name"],
            "description": role.get("description", ""),
            "policies": [
                {"name": p} for p in role.get("policies", [])
            ]
        }

        result = self._make_request("POST", "roles", role_data)
        return result is not None

    def create_team(self, team: dict, parent_id: str | None = None) -> str | None:
        """Create a team in OpenMetadata."""
        logger.info(f"Creating team: {team['name']}")

        team_data = {
            "name": team["name"],
            "displayName": team.get("displayName", team["name"]),
            "description": team.get("description", ""),
            "teamType": team.get("teamType", "Group"),
            "defaultRoles": [
                {"name": r} for r in team.get("defaultRoles", [])
            ]
        }

        if parent_id:
            team_data["parents"] = [{"id": parent_id}]

        result = self._make_request("POST", "teams", team_data)
        return result.get("id") if result else None

    def create_classification(self, classification: dict) -> bool:
        """Create a classification (tag category) in OpenMetadata."""
        logger.info(f"Creating classification: {classification['name']}")

        classification_data = {
            "name": classification["name"],
            "description": classification.get("description", ""),
        }

        result = self._make_request("POST", "classifications", classification_data)
        if not result:
            return False

        classification_fqn = classification["name"]

        # Create tags within the classification
        for tag in classification.get("tags", []):
            tag_data = {
                "name": tag["name"],
                "description": tag.get("description", ""),
                "classification": classification_fqn,
            }
            self._make_request("POST", "tags", tag_data)

        return True

    def initialize_from_config(self, config_path: str) -> bool:
        """Initialize RBAC from configuration file."""
        logger.info(f"Loading configuration from: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        success = True

        # Create policies first (roles depend on them)
        for policy in config.get("policies", []):
            if not self.create_policy(policy):
                logger.warning(f"Failed to create policy: {policy['name']}")
                success = False

        # Create roles (depend on policies)
        for role in config.get("roles", []):
            if not self.create_role(role):
                logger.warning(f"Failed to create role: {role['name']}")
                success = False

        # Create teams (may have hierarchy)
        team_ids = {}
        for team in config.get("teams", []):
            parent_name = team.get("parent")
            parent_id = team_ids.get(parent_name) if parent_name else None
            team_id = self.create_team(team, parent_id)
            if team_id:
                team_ids[team["name"]] = team_id
            else:
                logger.warning(f"Failed to create team: {team['name']}")
                success = False

        # Create classifications and tags
        for classification in config.get("classifications", []):
            if not self.create_classification(classification):
                logger.warning(
                    f"Failed to create classification: {classification['name']}"
                )
                success = False

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Initialize OpenMetadata RBAC configuration"
    )
    parser.add_argument(
        "--config",
        default="roles_config.yaml",
        help="Path to RBAC configuration file"
    )
    parser.add_argument(
        "--server",
        default=os.getenv("OPENMETADATA_SERVER", "http://localhost:8585"),
        help="OpenMetadata server URL"
    )
    parser.add_argument(
        "--token",
        default=os.getenv("OPENMETADATA_JWT_TOKEN"),
        help="JWT token for authentication"
    )
    parser.add_argument(
        "--username",
        default=os.getenv("OPENMETADATA_USERNAME"),
        help="Username for basic authentication"
    )
    parser.add_argument(
        "--password",
        default=os.getenv("OPENMETADATA_PASSWORD"),
        help="Password for basic authentication"
    )

    args = parser.parse_args()

    initializer = OpenMetadataRBACInitializer(
        args.server,
        args.token,
        args.username,
        args.password
    )
    success = initializer.initialize_from_config(args.config)

    if success:
        logger.info("RBAC initialization completed successfully")
        sys.exit(0)
    else:
        logger.error("RBAC initialization completed with errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
