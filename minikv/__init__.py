"""MiniKV — a log-structured key-value database, from scratch in pure Python."""

from .client import Client
from .storage import StorageEngine

__version__ = "1.0.0"
__all__ = ["StorageEngine", "Client", "__version__"]
