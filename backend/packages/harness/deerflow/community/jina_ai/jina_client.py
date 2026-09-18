import logging
import os

import httpx

logger = logging.getLogger(__name__)

_api_key_warned = False


#: Statuses r.jina.ai answers for the *caller*, not for the page: no key,
#: a refused key, a refused address, or a spent quota. Every address gets the
#: same answer, so the tool stamps them provider scope and the run withdraws
#: the tool (thirteen addresses, thirteen 401s).
PROVIDER_REFUSAL_STATUSES = frozenset({401, 402, 403, 429})


class JinaClient:
    #: The HTTP status of the last ``crawl``, or ``None`` before one and after
    #: a transport failure. Read by the tool to type the refusal; the string
    #: ``crawl`` returns stays what it was.
    last_status: int | None = None

    async def crawl(self, url: str, return_format: str = "html", timeout: int = 10, proxy: str | None = None, trust_env: bool = True) -> str:
        global _api_key_warned
        headers = {
            "Content-Type": "application/json",
            "X-Return-Format": return_format,
            "X-Timeout": str(timeout),
        }
        if os.getenv("JINA_API_KEY"):
            headers["Authorization"] = f"Bearer {os.getenv('JINA_API_KEY')}"
        elif not _api_key_warned:
            _api_key_warned = True
            logger.warning("Jina API key is not set. Provide your own key to access a higher rate limit. See https://jina.ai/reader for more information.")
        data = {"url": url}
        try:
            client_kwargs: dict[str, object] = {"trust_env": trust_env}
            if proxy:
                client_kwargs["proxy"] = proxy
            self.last_status = None
            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.post("https://r.jina.ai/", headers=headers, json=data, timeout=timeout)
            self.last_status = response.status_code

            if response.status_code != 200:
                error_message = f"Jina API returned status {response.status_code}: {response.text}"
                logger.error(error_message)
                return f"Error: {error_message}"

            if not response.text or not response.text.strip():
                error_message = "Jina API returned empty response"
                logger.error(error_message)
                return f"Error: {error_message}"

            return response.text
        except Exception as e:
            error_message = f"Request to Jina API failed: {type(e).__name__}: {e}"
            logger.warning(error_message)
            return f"Error: {error_message}"
