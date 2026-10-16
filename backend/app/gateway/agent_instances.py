"""One-shot AI assistant/AI employee operations; never Gateway startup."""

import argparse
import asyncio
import json
import math
import re
import sys

from app.gateway.agent_operator_contract import CommandFailed, CommandRefused

COMMANDS = ("list", "status", "verify", "close-workspace", "export", "supervise", "delete", "adopt-host", "settle")
_VALUED = frozenset(("--issuer", "--subject", "--email", "--wait-seconds", "--from", "--space", "--operation"))


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise CommandRefused(message)


def _identifier(value):
    if re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise argparse.ArgumentTypeError("must be 32 hexadecimal characters")
    return value


def _command_word(arguments):
    words = iter(arguments)
    for word in words:
        if word in _VALUED:
            next(words, None)
        elif word in COMMANDS:
            return word
    return None


def _parser():
    parser = _Parser(prog="python -m app.gateway.agent_instances", add_help=False)
    verbs = parser.add_subparsers(dest="command", required=True)
    for command in COMMANDS:
        verb = verbs.add_parser(command, add_help=False)
        if command == "list":
            verb.add_argument("--all", action="store_true")
        elif command in ("close-workspace", "export", "supervise", "delete"):
            verb.add_argument("instance_id", type=_identifier)
            verb.add_argument("--end-running-work", action="store_true")
            verb.add_argument("--wait-seconds", type=float, default=120.0)
            if command == "supervise":
                verb.add_argument("--issuer")
                verb.add_argument("--subject")
                verb.add_argument("--email")
        elif command == "adopt-host":
            verb.add_argument("--from", dest="old_host")
            verb.add_argument("--acknowledge-old-host-gone", action="store_true")
        elif command == "settle":
            verb.add_argument("--space", dest="space_id", type=_identifier, required=True)
            verb.add_argument("--operation", dest="operation_id", type=_identifier, required=True)
            verb.add_argument("--discard", action="store_true")
    return parser


async def _run(command, **options):
    from app.gateway.agent_operator_services import run

    return await run(command, **options)


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    command = _command_word(arguments)
    try:
        parser = _parser()
        options = vars(parser.parse_args(arguments))
        command = options.pop("command")
        wait = options.get("wait_seconds")
        if wait is not None and (not math.isfinite(wait) or wait < 0):
            parser.error("--wait-seconds must be a finite non-negative number")
        if command == "supervise":
            if options["email"] is not None:
                if options["issuer"] is not None or options["subject"] is not None:
                    parser.error("--email cannot be combined with --issuer or --subject")
            elif options["issuer"] is None or options["subject"] is None:
                parser.error("give --issuer and --subject, or --email")
        document = asyncio.run(_run(command, **options))
    except CommandRefused as exc:
        if isinstance(exc, CommandFailed):
            print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        document = {"command": command, "returncode": 1, "error": str(exc), "note": exc.note, **exc.fields}
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"
        print(text, file=sys.stderr)
        document = {"command": command, "returncode": 1, "error": text, "note": "failed; run the verb again"}
    print(json.dumps(document, sort_keys=True))
    return document["returncode"]


if __name__ == "__main__":
    raise SystemExit(main())
