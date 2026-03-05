import os
import re
import subprocess
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.parse import quote

from netunicorn.base import Architecture, Failure, Node, Success, Task, TaskDispatcher

from netunicorn.library.tasks.tasks_utils import subprocess_run


class UploadToWebDav(TaskDispatcher):
    def __init__(
        self,
        filepaths: Iterable[str],
        endpoint: str,
        username: str,
        password: str,
        directory: str = "",
        directory_parts: Optional[Sequence[str]] = None,
        info: Optional[Dict[str, str]] = None,
        node_env_keys: Optional[Sequence[str]] = None,
        *args,
        **kwargs,
    ):
        self.filepaths = list(filepaths)
        self.endpoint = endpoint.rstrip("/")
        self.username = username
        self.password = password
        self.directory = directory
        self.directory_parts = list(directory_parts or [])
        self.info = dict(info or {})
        self.node_env_keys = list(
            node_env_keys or UploadToWebDavImplementation.DEFAULT_NODE_ENV_KEYS
        )

        super().__init__(*args, **kwargs)

        self.linux_implementation = UploadToWebDavImplementation(
            filepaths=self.filepaths,
            endpoint=self.endpoint,
            username=self.username,
            password=self.password,
            directory=self.directory,
            directory_parts=self.directory_parts,
            info=self.info,
            node_env_keys=self.node_env_keys,
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
    DEFAULT_NODE_ENV_KEYS = (
        "NETUNICORN_NODE_NAME",
        "NETUNICORN_NODE_ID",
        "NETUNICORN_DEPLOYMENT_NODE_NAME",
        "HOSTNAME",
    )

    def __init__(
        self,
        filepaths: Iterable[str],
        endpoint: str,
        username: str,
        password: str,
        directory: str = "",
        directory_parts: Optional[Sequence[str]] = None,
        info: Optional[Dict[str, str]] = None,
        node_env_keys: Optional[Sequence[str]] = None,
        *args,
        **kwargs,
    ):
        self.filepaths = list(filepaths)
        self.endpoint = endpoint.rstrip("/")
        self.username = username
        self.password = password
        self.directory = directory
        self.directory_parts = list(directory_parts or [])
        self.info = dict(info or {})
        self.node_env_keys = list(node_env_keys or self.DEFAULT_NODE_ENV_KEYS)
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
        sanitized = [self._sanitize_segment(p) for p in parts if p is not None]
        sanitized = [p for p in sanitized if p]
        if not sanitized:
            return f"{self.endpoint}/"
        encoded = [quote(p, safe="._-") for p in sanitized]
        return f"{self.endpoint}/{'/'.join(encoded)}/"

    def _detect_node(self) -> str:
        for key in self.node_env_keys:
            value = os.environ.get(key)
            if value:
                return self._sanitize_segment(value)
        return "unknown-node"

    def _mkcol(self, folder_url: str, auth: str) -> bool:
        # subprocess.run used directly to inspect the HTTP status code
        process = subprocess.run(
            [
                "curl",
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
            ],
            capture_output=True,
            text=True,
        )
        return (process.stdout or "").strip() in {"201", "301", "405"}

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
        auth = f"{self.username}:{self.password}"
        context = self._build_context()

        path_parts: List[str] = [self.directory]
        path_parts += [self._resolve_part(p, context) for p in self.directory_parts]
        path_parts = [p for p in path_parts if p]

        for depth in range(1, len(path_parts) + 1):
            self._mkcol(self._join_webdav_url(*path_parts[:depth]), auth)

        base_url = self._join_webdav_url(*path_parts)
        results = []

        for filepath in self.filepaths:
            filename = os.path.basename(filepath)
            if not filename or not os.path.exists(filepath):
                results.append(Failure(f"Missing local file: {filepath}"))
                continue

            dest_url = f"{base_url}{quote(filename, safe='._-')}"
            result = subprocess_run(
                ["curl", "--fail", "-sS", "-u", auth, "-T", filepath, dest_url]
            )
            if isinstance(result, Success):
                results.append(Success(f"Uploaded to: {dest_url}"))
            else:
                results.append(
                    Failure(f"Upload failed for {filename}: {result.failure()}")
                )

        container_type = (
            Success if all(isinstance(r, Success) for r in results) else Failure
        )
        return container_type(results)
