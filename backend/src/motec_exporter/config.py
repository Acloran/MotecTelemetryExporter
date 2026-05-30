from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    telemetry_source: str = Field(default="sample", alias="TELEMETRY_SOURCE")

    orion_db_host: str = Field(default="192.168.1.109", alias="ORION_DB_HOST")
    orion_db_port: int = Field(default=5432, alias="ORION_DB_PORT")
    orion_db_name: str = Field(default="orion", alias="ORION_DB_NAME")
    orion_db_user: str = Field(default="analysis", alias="ORION_DB_USER")
    orion_db_password: str = Field(default="", alias="ORION_DB_PASSWORD")
    orion_db_sslmode: str = Field(default="disable", alias="ORION_DB_SSLMODE")
    orion_db_connect_timeout: int = Field(default=5, alias="ORION_DB_CONNECT_TIMEOUT")
    angelique_db_host: str = Field(default="192.168.1.109", alias="ANGELIQUE_DB_HOST")
    angelique_db_port: int = Field(default=5432, alias="ANGELIQUE_DB_PORT")
    angelique_db_name: str = Field(default="angelique", alias="ANGELIQUE_DB_NAME")
    angelique_db_user: str = Field(default="", alias="ANGELIQUE_DB_USER")
    angelique_db_password: str = Field(default="", alias="ANGELIQUE_DB_PASSWORD")
    angelique_db_sslmode: str = Field(default="", alias="ANGELIQUE_DB_SSLMODE")
    angelique_db_connect_timeout: int = Field(default=0, alias="ANGELIQUE_DB_CONNECT_TIMEOUT")

    export_dir: Path = Field(default=ROOT_DIR / "exports", alias="EXPORT_DIR")
    track_dir: Path = Field(default=ROOT_DIR / "tracks", alias="TRACK_DIR")
    channel_chart_dir: Path = Field(default=ROOT_DIR / "channel_charts", alias="CHANNEL_CHART_DIR")
    cache_dir: Path = Field(default=ROOT_DIR / ".cache", alias="CACHE_DIR")
    display_timezone: str = Field(default="America/Chicago", alias="DISPLAY_TIMEZONE")
    kafka_mode: str = Field(default="local", alias="KAFKA_MODE")
    kafka_bootstrap_servers: str = Field(default="192.168.1.109:29092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_prefix: str = Field(default="grafana_data", alias="KAFKA_TOPIC_PREFIX")
    kafka_consumer_timeout_ms: int = Field(default=1000, alias="KAFKA_CONSUMER_TIMEOUT_MS")
    live_mqtt_host: str = Field(default="18.191.225.118", alias="LIVE_MQTT_HOST")
    live_mqtt_port: int = Field(default=1883, alias="LIVE_MQTT_PORT")
    live_mqtt_topic: str = Field(default="orion", alias="LIVE_MQTT_TOPIC")

    max_preview_points: int = Field(default=5000, alias="MAX_PREVIEW_POINTS")
    max_preview_seconds: int = Field(default=60 * 60 * 2, alias="MAX_PREVIEW_SECONDS")
    max_auto_split_seconds: int = Field(default=60 * 30, alias="MAX_AUTO_SPLIT_SECONDS")
    max_export_seconds: int = Field(default=60 * 30, alias="MAX_EXPORT_SECONDS")

    @property
    def use_postgres(self) -> bool:
        return self.telemetry_source.strip().lower() == "postgres"

    @property
    def resolved_export_dir(self) -> Path:
        return self._resolve_local(self.export_dir)

    @property
    def resolved_track_dir(self) -> Path:
        return self._resolve_local(self.track_dir)

    @property
    def resolved_channel_chart_dir(self) -> Path:
        return self._resolve_local(self.channel_chart_dir)

    @property
    def resolved_cache_dir(self) -> Path:
        return self._resolve_local(self.cache_dir)

    def _resolve_local(self, path: Path) -> Path:
        if path.is_absolute():
            return path
        return ROOT_DIR / path

    def for_source(self, source: str | None) -> "Settings":
        normalized = (source or "orion").strip().lower()
        if normalized not in {"orion", "angelique"}:
            raise ValueError(f"Unknown telemetry source: {source}")
        if normalized == "orion":
            return self.model_copy(update={"orion_db_name": self.orion_db_name})
        return self.model_copy(
            update={
                "orion_db_host": self.angelique_db_host or self.orion_db_host,
                "orion_db_port": self.angelique_db_port or self.orion_db_port,
                "orion_db_name": self.angelique_db_name or "angelique",
                "orion_db_user": self.angelique_db_user or self.orion_db_user,
                "orion_db_password": self.angelique_db_password or self.orion_db_password,
                "orion_db_sslmode": self.angelique_db_sslmode or self.orion_db_sslmode,
                "orion_db_connect_timeout": self.angelique_db_connect_timeout or self.orion_db_connect_timeout,
            }
        )


settings = Settings()
