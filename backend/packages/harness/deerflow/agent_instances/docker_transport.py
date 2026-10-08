"""Keep the existing AIO protocol inside one immutable Docker environment."""

import base64
import json
import math
import re
import subprocess

import httpx

_MAX_REQUEST = 16 * 1024 * 1024
_MAX_RESPONSE = 64 * 1024 * 1024

_FORWARD = r"""
import base64, json, sys, urllib.error, urllib.request
data = json.load(sys.stdin)
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args): return None
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
request = urllib.request.Request(data['url'], data=base64.b64decode(data['body']), headers=dict(data['headers']), method=data['method'])
try:
    response = opener.open(request, timeout=data['timeout'])
except urllib.error.HTTPError as error:
    response = error
with response:
    body = response.read(data['limit'] + 1)
    if len(body) > data['limit']: raise ValueError('Control response exceeds limit')
    print(json.dumps({'status': response.status, 'headers': list(response.headers.items()), 'body': base64.b64encode(body).decode('ascii')}))
"""


class DockerControlTransport(httpx.BaseTransport):
    def __init__(self, container_id, *, runner=subprocess.run):
        if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise ValueError("Control transport requires an exact immutable Docker identity")
        self.container_id, self.runner = container_id, runner

    def handle_request(self, request):
        if request.url.scheme != "http" or request.url.host != "127.0.0.1" or request.url.port != 8080:
            raise httpx.TransportError("Instance control cannot leave its admitted environment", request=request)
        body = request.read()
        if len(body) > _MAX_REQUEST:
            raise httpx.TransportError("Instance control request exceeds its byte limit", request=request)
        timeout = (request.extensions.get("timeout") or {}).get("read")
        timeout = timeout if isinstance(timeout, (int, float)) and math.isfinite(timeout) and timeout > 0 else 600
        timeout = min(timeout, 600)
        payload = {"url": str(request.url), "method": request.method, "headers": list(request.headers.multi_items()), "body": base64.b64encode(body).decode("ascii"), "timeout": timeout, "limit": _MAX_RESPONSE}
        try:
            result = self.runner(["docker", "exec", "-i", self.container_id, "python3", "-c", _FORWARD], input=json.dumps(payload).encode("utf-8"), capture_output=True, timeout=timeout + 5)
            if result.returncode or len(result.stdout) > 2 * _MAX_RESPONSE:
                raise ValueError("Exact environment control failed")
            response = json.loads(result.stdout)
            content = base64.b64decode(response["body"], validate=True)
            if len(content) > _MAX_RESPONSE or type(response["status"]) is not int or not 100 <= response["status"] <= 599:
                raise ValueError("Invalid control response")
            return httpx.Response(response["status"], headers=response["headers"], content=content, request=request)
        except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as exc:
            raise httpx.TransportError("The exact admitted instance environment is unavailable", request=request) from exc
