"""Small storage abstraction shared by local and ADLS batch collectors."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Protocol


class LakeStorage(Protocol):
    def write_bytes(self, zone: str, path: str, payload: bytes) -> str: ...


def _safe_relative_path(path: str) -> str:
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"Data Lake path must be relative: {path}")
    return candidate.as_posix()


class LocalLakeStorage:
    def __init__(self, root: str | Path = "data_lake") -> None:
        self.root = Path(root).resolve()

    def write_bytes(self, zone: str, path: str, payload: bytes) -> str:
        relative = _safe_relative_path(path)
        target = self.root / zone / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_name, target)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise
        return str(target)


class AdlsLakeStorage:
    """ADLS Gen2 writer using DefaultAzureCredential or a VM identity."""

    def __init__(self, account_name: str) -> None:
        if not account_name:
            raise ValueError("AZURE_STORAGE_ACCOUNT is required for the ADLS backend")
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as error:
            raise RuntimeError(
                "Install requirements-azure.txt before using the ADLS backend"
            ) from error
        self.account_name = account_name
        self.service = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=DefaultAzureCredential(),
        )

    def write_bytes(self, zone: str, path: str, payload: bytes) -> str:
        relative = _safe_relative_path(path)
        filesystem = self.service.get_file_system_client(zone)
        file_client = filesystem.get_file_client(relative)
        file_client.upload_data(payload, overwrite=True)
        return f"abfss://{zone}@{self.account_name}.dfs.core.windows.net/{relative}"


def create_lake_storage(
    backend: str,
    root: str | Path = "data_lake",
    account_name: str | None = None,
) -> LakeStorage:
    backend = backend.lower().strip()
    if backend == "local":
        return LocalLakeStorage(root)
    if backend == "adls":
        return AdlsLakeStorage(account_name or os.environ.get("AZURE_STORAGE_ACCOUNT", ""))
    raise ValueError(f"Unsupported Data Lake backend: {backend}")
