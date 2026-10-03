import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    api_key: str
    base_url: str = "https://api.electricitymaps.com/v4"
    zone: str = "FR"
    request_timeout_seconds: int = 30
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "Settings":
        api_key = os.getenv("ELECTRICITY_MAPS_API_KEY")
        if not api_key:
            raise RuntimeError("ELECTRICITY_MAPS_API_KEY is not set")

        return cls(
            api_key=api_key,
            base_url=os.getenv(
                "ELECTRICITY_MAPS_BASE_URL",
                "https://api.electricitymaps.com/v4",
            ).rstrip("/"),
            zone=os.getenv("ELECTRICITY_MAPS_ZONE", "FR"),
        )
