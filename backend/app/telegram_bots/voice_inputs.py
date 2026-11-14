"""Route optional transcript consumers through their durable captured owner."""


class VoiceInputRouter:
    def __init__(self, fallback, owners):
        self.fallback, self.owners = fallback, dict(owners)

    def owner(self, origin):
        value = origin.route_basis.get("work_voice", {})
        return self.owners.get(value.get("owner"), self.fallback)

    async def capture(self, channel, link, origin, **kwargs):
        for owner in self.owners.values():
            captured = await owner.capture(channel, link, origin, **kwargs)
            if captured is not None:
                return captured
        return await self.fallback.capture(channel, link, origin, **kwargs)

    async def prepare_complete(self, link, origin):
        prepare = getattr(self.owner(origin), "prepare_complete", None)
        return await prepare(link, origin) if prepare is not None else False

    async def complete(self, link, origin, **kwargs):
        return await self.owner(origin).complete(link, origin, **kwargs)

    async def resume(self, channel, origin, **kwargs):
        resume = getattr(self.owner(origin), "resume", None)
        return await resume(channel, origin, **kwargs) if resume is not None else False
