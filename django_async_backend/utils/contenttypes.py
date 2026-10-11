from typing import (
    TYPE_CHECKING,
    Type,
)

from django.db import DEFAULT_DB_ALIAS

if TYPE_CHECKING:
    from django.contrib.contenttypes.models import ContentType
    from django.db.models import Model


async def aget_for_model(
    model: "Type[Model]",
    using: str = DEFAULT_DB_ALIAS,
    for_concrete_model: bool = True,
) -> "ContentType":
    from django.contrib.contenttypes.models import ContentType

    types = ContentType.objects.db_manager(using)
    using = types.db
    opts = types._get_opts(model, for_concrete_model)

    try:
        return types._get_from_cache(opts)
    except KeyError:
        pass

    lookup = {"app_label": opts.app_label, "model": opts.model_name}
    manager = ContentType.async_objects.using(using)

    try:
        content_type = await manager.aget(**lookup)
    except ContentType.DoesNotExist:
        content_type, _ = await manager.aget_or_create(**lookup)

    types._add_to_cache(using, content_type)
    return content_type


async def aget_for_id(
    id: int,
    using: str = DEFAULT_DB_ALIAS,
) -> "ContentType":
    from django.contrib.contenttypes.models import ContentType

    types = ContentType.objects.db_manager(using)
    using = types.db
    try:
        return types._cache[using][id]
    except KeyError:
        pass

    content_type = await ContentType.async_objects.using(using).aget(pk=id)
    types._add_to_cache(using, content_type)
    return content_type
