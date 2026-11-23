"""Translate A1 references in shared formula text without evaluating expressions."""

import re
from dataclasses import dataclass

# Keep quoted text, sheet names, and identifiers whole. Square brackets enclose
# external workbook names or structured references and can nest.
_TOKEN = re.compile(r""""(?:""|[^"])*"|'(?:''|[^'])*'|[\w.$?\\\u0080-\U0010FFFF]+|.""", re.DOTALL)
_CELL = re.compile(r"(\$?)([A-Za-z]{1,3})(\$?)([1-9][0-9]{0,6})")
_COL = re.compile(r"(\$?)([A-Za-z]{1,3})")
_ROW = re.compile(r"(\$?)([1-9][0-9]{0,6})")
_MAX_COL = 16_384
_MAX_ROW = 1_048_576


def column_number(letters):
    number = 0
    for letter in letters.upper():
        number = number * 26 + ord(letter) - ord("A") + 1
    return number


def _column_letters(number):
    letters = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


@dataclass(frozen=True, slots=True)
class _Reference:
    lock_col: str
    letters: str
    lock_row: str
    digits: str

    def shifted(self, dr, dc):
        letters, digits = self.letters, self.digits
        if letters and not self.lock_col and dc:
            column = column_number(letters) + dc
            if not 1 <= column <= _MAX_COL:
                return "#REF!"
            letters = _column_letters(column)
        if digits and not self.lock_row and dr:
            row = int(digits) + dr
            if not 1 <= row <= _MAX_ROW:
                return "#REF!"
            digits = str(row)
        return self.lock_col + letters + self.lock_row + digits


def _reference(token, axis):
    if cell := _CELL.fullmatch(token):
        ref = _Reference(*cell.groups())
    elif axis is _COL:
        ref = _Reference(*_COL.fullmatch(token).groups(), "", "")
    elif axis is _ROW:
        ref = _Reference("", "", *_ROW.fullmatch(token).groups())
    else:
        return None
    if column_number(ref.letters) > _MAX_COL or int(ref.digits or "0") > _MAX_ROW:
        return None  # Outside the grid, this token can be a defined name.
    return ref


def _tokens(formula):
    pos = 0
    while pos < len(formula):
        if formula[pos] == "[":
            # A single quote escapes a special character inside a table header.
            start = pos
            depth = 1
            pos += 1
            while pos < len(formula) and depth:
                char = formula[pos]
                pos += 1
                if char == "'":
                    pos = min(pos + 1, len(formula))
                elif char == "[":
                    depth += 1
                elif char == "]":
                    depth -= 1
            yield formula[start:pos]
        else:
            match = _TOKEN.match(formula, pos)
            yield match[0]
            pos = match.end()


def _axis_pair(first, last):
    if _COL.fullmatch(first) and _COL.fullmatch(last):
        if max(column_number(word.lstrip("$")) for word in (first, last)) <= _MAX_COL:
            return _COL
    if _ROW.fullmatch(first) and _ROW.fullmatch(last):
        if max(int(word.lstrip("$")) for word in (first, last)) <= _MAX_ROW:
            return _ROW
    return None


def _parts(formula):
    tokens = list(_tokens(formula))
    meaningful = [token for token in tokens if not token.isspace()]
    index = 0
    literal = []
    for position, token in enumerate(tokens):
        if token.isspace():
            literal.append(token)
            continue
        after = meaningful[index + 1 : index + 4]
        before = meaningful[max(0, index - 2) : index]
        index += 1
        # Preserve sheet qualifiers (including both ends of a 3D qualifier),
        # function names such as LOG10, and table names.
        qualifier = len(after) == 3 and after[0] == ":" and after[2] == "!"
        next_token = tokens[position + 1] if position + 1 < len(tokens) else ""
        function_or_table = next_token == "(" or next_token.startswith("[")
        if function_or_table or qualifier or (after and after[0] == "!"):
            literal.append(token)
            continue
        axis = None
        if len(after) >= 2 and after[0] == ":":
            axis = _axis_pair(token, after[1])
        if axis is None and len(before) == 2 and before[1] == ":":
            axis = _axis_pair(token, before[0])
        ref = _reference(token, axis)
        if ref is None:
            literal.append(token)
        else:
            if literal:
                yield "".join(literal)
                literal.clear()
            yield ref
    if literal:
        yield "".join(literal)


def compile_formula(formula):
    return tuple(_parts(formula))


@dataclass(frozen=True, slots=True)
class SharedFormula:
    row: int
    col: int
    area: tuple[int, int, int, int]
    formula: str

    def at(self, row, col, compiled):
        top, left, bottom, right = self.area
        if not self.formula or not (top <= row <= bottom and left <= col <= right):
            raise ValueError("Invalid shared formula range")
        dr, dc = row - self.row, col - self.col
        return "".join(part.shifted(dr, dc) if isinstance(part, _Reference) else part for part in compiled(self.formula))


def coordinates(ref):
    match = _CELL.fullmatch(ref)
    if not match:
        raise ValueError("Invalid shared formula reference")
    return int(match[4]), column_number(match[2])


def bounds(ref):
    first, _, last = ref.partition(":")
    return *coordinates(first), *coordinates(last or first)


def group_index(value):
    digits = value.strip().removeprefix("+")
    if not digits.isascii() or not digits.isdecimal():
        raise ValueError("Invalid shared formula index")
    digits = digits.lstrip("0") or "0"
    if len(digits) > 10 or int(digits) > 0xFFFFFFFF:
        raise ValueError("Invalid shared formula index")
    return int(digits)
