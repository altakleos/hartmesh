"""A caller keeps the captured resource and operation ID with its own intent."""

from deerflow_extension_api import ResourceReference, StorageProvider, StorageResource, StorageUnsupported


async def create_file(provider: StorageProvider, *, resource: StorageResource, path: str, content: bytes, operation_id: str) -> ResourceReference:
    """Create ordinary data under the current host actor; propagate pending outcomes."""
    if not provider.capabilities.available or not provider.capabilities.files:
        raise StorageUnsupported("The host does not provide resource file storage")
    storage = await provider.current()
    revision = await storage.write(space_id=resource.id, generation=resource.generation, operation_id=operation_id, path=path, content=content, create=True)
    return ResourceReference(resource.id, path, revision)
