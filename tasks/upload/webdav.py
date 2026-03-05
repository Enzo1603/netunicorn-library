import os
import re
import subprocess
from typing import Dict, Iterable, Literal, Optional, Sequence
from urllib.parse import quote

from netunicorn.base import Architecture, Failure, Node, Success, Task, TaskDispatcher


class UploadToWebDav(TaskDispatcher):
    """
    Dispatcher that selects a platform-specific WebDAV uploader implementation.
    """

    def __init__(
        self,
        filepaths: Iterable[str],
        endpoint: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        authentication: Literal["basic"] = "basic",
        directory: str = "",
        directory_parts: Optional[Sequence[str]] = None,
        info: Optional[Dict[str, str]] = None,
        node_env_keys: Optional[Sequence[str]] = None,
        curl_bin: str = "curl",
        url: Optional[str] = None,
        user: Optional[str] = None,
        *args,
        **kwargs,
    ):
        # Support both old and smart argument names
        resolved_endpoint = (endpoint or url or "").rstrip("/")
        resolved_username = username if username is not None else user

        self.filepaths = list(filepaths)
        self.endpoint = resolved_endpoint
        self.username = resolved_username
        self.password = password
        self.authentication = authentication
        self.directory = directory
        self.directory_parts = list(directory_parts or [])
        self.info = dict(info or {})
        self.node_env_keys = tuple(
            node_env_keys or UploadToWebDavImplementation.DEFAULT_NODE_ENV_KEYS
        )
        self.curl_bin = curl_bin

        super().__init__(*args, **kwargs)

        self.linux_implementation = UploadToWebDavImplementation(
            filepaths=self.filepaths,
            endpoint=self.endpoint,
            username=self.username,
            password=self.password,
            authentication=self.authentication,
            directory=self.directory,
            directory_parts=self.directory_parts,
            info=self.info,
            node_env_keys=self.node_env_keys,
            curl_bin=self.curl_bin,
            name=self.name,
        )
        self.linux_implementation.requirements = ["sudo apt-get install -y curl"]

    def dispatch(self, node: Node) -> Task:
        if node.architecture in {Architecture.LINUX_AMD64, Architecture.LINUX_ARM64}:
            return self.linux_implementation

        raise NotImplementedError(
            f"UploadToWebDav is not implemented for {node.architecture}"
        )


class UploadToWebDavImplementation(Task):
    """
    Linux implementation with smart WebDAV upload behavior:
      - dynamic context-based folder paths
      - MKCOL folder creation
      - per-file success/failure details
    """

    DEFAULT_NODE_ENV_KEYS = (
        "NETUNICORN_NODE_NAME",
        "NETUNICORN_NODE_ID",
        "NETUNICORN_DEPLOYMENT_NODE_NAME",
        "HOSTNAME",
    )

    def __init__(
        self,
        filepaths: Iterable[str],
        endpoint: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        authentication: Literal["basic"] = "basic",
        directory: str = "",
        directory_parts: Optional[Sequence[str]] = None,
        info: Optional[Dict[str, str]] = None,
        node_env_keys: Optional[Sequence[str]] = None,
        curl_bin: str = "curl",
        url: Optional[str] = None,
        user: Optional[str] = None,
        *args,
        **kwargs,
    ):
        # Support both old and smart argument names
        resolved_endpoint = (endpoint or url or "").rstrip("/")
        resolved_username = username if username is not None else user

        self.filepaths = list(filepaths)
        self.endpoint = resolved_endpoint
        self.username = resolved_username
        self.password = password
        self.authentication = authentication
        self.directory = directory
        self.directory_parts = list(directory_parts or [])
        self.info = dict(info or {})
        self.node_env_keys = tuple(node_env_keys or self.DEFAULT_NODE_ENV_KEYS)
        self.curl_bin = curl_bin

        super().__init__(*args, **kwargs)

    @staticmethod
    def _sanitize_segment(value: object) -> str:
        text = str(value).strip()
        if not text:
            return ""
        text = text.replace("/", "_")
        text = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
        return text.strip("._-") or "unknown"

    def _join_webdav_url(self, *parts: object) -> str:
        sanitized = [self._sanitize_segment(part) for part in parts if part is not None]
        sanitized = [part for part in sanitized if part]
        if not sanitized:
            return f"{self.endpoint}/"
        encoded = [quote(part, safe="._-") for part in sanitized]
        return f"{self.endpoint}/{'/'.join(encoded)}/"

    def _detect_node(self) -> str:
        for env_key in self.node_env_keys:
            candidate = os.environ.get(env_key)
            if candidate:
                return self._sanitize_segment(candidate)
        return "unknown-node"

    def _mkcol(self, folder_url: str, auth: str) -> bool:
        command = [
            self.curl_bin,
            "-sS",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-X",
            "MKCOL",
            "--user",
            auth,
            folder_url,
        ]
        process = subprocess.run(command, capture_output=True, text=True)
        status_code = (process.stdout or "").strip()
        return status_code in {"201", "301", "405"}

    def _build_context(self) -> Dict[str, str]:
        context: Dict[str, str] = {
            "executor_id": self._sanitize_segment(
                os.environ.get("NETUNICORN_EXECUTOR_ID") or "unknown-executor"
            ),
            "node": self._detect_node(),
            "region": self._sanitize_segment(
                os.environ.get("AWS_REGION")
                or os.environ.get("AWS_DEFAULT_REGION")
                or "unknown-region"
            ),
        }
        for key, value in self.info.items():
            context[key] = self._sanitize_segment(value)
        return context

    def _resolve_part(self, part: object, context: Dict[str, str]) -> str:
        text = str(part)
        for key, value in context.items():
            text = text.replace(f"{{{key}}}", value)
        return self._sanitize_segment(text)

    def run(self):
        if not self.endpoint:
            return Failure("Missing WebDAV endpoint/url")
        if self.authentication != "basic":
            return Failure(f"Unsupported authentication type: {self.authentication}")
        if not self.username or not self.password:
            return Failure("Missing WebDAV credentials")

        auth = f"{self.username}:{self.password}"
        context = self._build_context()

        path_parts = [self.directory]
        path_parts.extend(
            self._resolve_part(part, context) for part in self.directory_parts
        )
        path_parts = [part for part in path_parts if part]

        # Attempt folder creation (best effort)
        for depth in range(1, len(path_parts) + 1):
            folder_url = self._join_webdav_url(*path_parts[:depth])
            self._mkcol(folder_url, auth)

        base_folder_url = self._join_webdav_url(*path_parts)
        results = []

        for filepath in self.filepaths:
            filename = os.path.basename(filepath)
            if not filename or not os.path.exists(filepath):
                results.append(Failure(f"Missing local file: {filepath}"))
                continue

            dest_url = f"{base_folder_url}{quote(filename, safe='._-')}"
            command = [
                self.curl_bin,
                "--fail",
                "-sS",
                "-u",
                auth,
                "-T",
                filepath,
                dest_url,
            ]
            process = subprocess.run(command, capture_output=True, text=True)

            if process.returncode == 0:
                results.append(Success(f"Uploaded to: {dest_url}"))
            else:
                error_message = (
                    process.stderr or process.stdout or "upload failed"
                ).strip()
                results.append(
                    Failure(f"Upload failed for {filename}: {error_message}")
                )

        container_type = (
            Success
            if all(isinstance(result, Success) for result in results)
            else Failure
        )
        return container_type(results)
