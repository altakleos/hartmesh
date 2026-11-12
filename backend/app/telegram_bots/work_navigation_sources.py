"""Bind each full preview family to its exact current navigation member."""

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import require_request_origin
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_navigation_origin import NavigationOriginProjection


def delivery_scope(item, family, ordinary_class, ordinary_key):
    origin = item.payload.get(family, {}).get("navigation_origin")
    if origin is None:
        return ordinary_class, ordinary_key
    from app.telegram_bots.work_navigation_origin import validate_origin

    validate_origin(origin)
    return "reply", "notice:work-navigation:" + origin["query_key"]


async def qualify_navigation(navigation, captured, basis, source, projection, *, family):
    if "navigation_origin" not in basis:
        return projection
    if navigation is None:
        raise ProductSourceRetired()
    origin = await navigation.restore(captured.link, basis["navigation_origin"])
    if family != origin.target.family:
        raise ProductSourceRetired()
    original = origin.target.source
    if family == "work_request_basis":
        if origin.target.review != ("result_files" in basis) or origin.target.routing != (basis["version"] in {5, 6}):
            raise ProductSourceRetired()
        if basis["version"] in {5, 6} and basis["routing"].get("response_id") != original.routing.get("response_id"):
            raise ProductSourceRetired()
        require_request_origin(original, source)
    elif family == "work_news_basis":
        from app.telegram_bots.work_news_sources import same_news_origin

        if not same_news_origin(original, source):
            raise ProductSourceRetired()
    elif family == "result_notice_basis":
        if original.event_signature != source.event_signature or not same_body(original.identity, source.identity):
            raise ProductSourceRetired()
    else:
        raise ProductSourceRetired()
    return DigestProjection(captured.link, [projection, NavigationOriginProjection(origin)])
