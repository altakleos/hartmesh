/**
 * The report card's formatting, kept identical to the skill's renderers.
 *
 * `business_report_common.format_value` is the one function the PDF, DOCX,
 * XLSX and HTML renders share, so a number reads the same in every download.
 * The card is a fifth render of the same `report.json` and has to agree with
 * them, which rules out `Intl.NumberFormat`: it rounds the *binary* double, so
 * a value the document prints as 8.4 can come out 8.3 here. Python rounds
 * `Decimal(str(value))` — the shortest decimal spelling of the double, ties
 * away from zero — and so does this module, on the string.
 *
 * Grouping is a plain comma and the decimal separator a point, because the
 * documents are written that way (`f"{value:,.2f}"`) whatever `meta.lang` says.
 */

/** `CURRENCY_SYMBOLS` in `business_report_common.py`. */
const CURRENCY_SYMBOLS: Readonly<Record<string, string>> = {
  USD: "$",
  EUR: "€",
  GBP: "£",
  CAD: "CA$",
  AUD: "A$",
  NZD: "NZ$",
  JPY: "¥",
  INR: "₹",
};

/** What a missing number renders as in every render. */
const MISSING = "—";

const CONTROL_CHARS = /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g;

export type ReportFormat =
  | "currency"
  | "integer"
  | "number"
  | "percent"
  | "text"
  | "date";

export type ReportCell = number | string | null;

/** A number in plain decimal notation, so string rounding can read its digits. */
function toPlainDecimalString(value: number) {
  const text = String(value);
  const scientific = /^(-?)(\d+)(?:\.(\d+))?e([+-]\d+)$/i.exec(text);
  if (!scientific) {
    return text;
  }
  const [, sign = "", whole = "0", fraction = "", exponent = "0"] = scientific;
  const digits = `${whole}${fraction}`;
  const pointIndex = whole.length + Number(exponent);
  if (pointIndex <= 0) {
    return `${sign}0.${"0".repeat(-pointIndex)}${digits}`;
  }
  if (pointIndex >= digits.length) {
    return `${sign}${digits}${"0".repeat(pointIndex - digits.length)}`;
  }
  return `${sign}${digits.slice(0, pointIndex)}.${digits.slice(pointIndex)}`;
}

/** Add one to a non-negative decimal digit string, growing it on carry. */
function incrementDigits(digits: string) {
  const result = digits.split("");
  for (let index = result.length - 1; index >= 0; index -= 1) {
    if (result[index] !== "9") {
      result[index] = String(Number(result[index]) + 1);
      return result.join("");
    }
    result[index] = "0";
  }
  return `1${result.join("")}`;
}

/**
 * `Decimal(str(value)).quantize(…, ROUND_HALF_UP)` for a magnitude.
 *
 * Ties go away from zero, so the caller rounds `Math.abs(value)` and puts the
 * sign back. Returns the digits either side of the point, ungrouped.
 */
function roundMagnitude(value: number, places: number) {
  const plain = toPlainDecimalString(Math.abs(value));
  const [whole = "0", fraction = ""] = plain.split(".");
  if (fraction.length <= places) {
    return { whole, fraction: fraction.padEnd(places, "0") };
  }
  const kept = `${whole}${fraction.slice(0, places)}`;
  // ROUND_HALF_UP keeps the digit when the discarded remainder is below a
  // half, which is exactly "the next digit is under 5".
  const rounded =
    fraction.charCodeAt(places) - 48 >= 5 ? incrementDigits(kept) : kept;
  const cut = rounded.length - places;
  return { whole: rounded.slice(0, cut), fraction: rounded.slice(cut) };
}

/** `f"{…:,}"`: a comma every three digits. */
function groupThousands(whole: string) {
  return whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/** The rounded magnitude, grouped, with no sign of its own. */
function magnitude(value: number, places: number) {
  const { whole, fraction } = roundMagnitude(value, places);
  return places === 0
    ? groupThousands(whole)
    : `${groupThousands(whole)}.${fraction}`;
}

function isNegative(value: number) {
  return value < 0 || Object.is(value, -0);
}

/** `to_text`: the cell as a person reads it, with no `10001.0` for an id. */
export function toText(value: ReportCell | boolean | undefined) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "number") {
    return String(value);
  }
  return value.replace(CONTROL_CHARS, "").trim();
}

function currencyPrefix(currency: string) {
  return CURRENCY_SYMBOLS[currency] ?? `${currency} `;
}

/**
 * The skill's `format_value`, with one deliberate difference: a non-numeric
 * cell in a numeric column renders as its own text instead of raising. The
 * documents cannot be built at all in that case, so there is nothing to agree
 * with, and a card that shows the cell beats a card that shows nothing.
 */
export function formatValue(
  value: ReportCell,
  format: ReportFormat,
  currency = "USD",
): string {
  if (format === "text") {
    return toText(value);
  }
  if (value === null) {
    return MISSING;
  }
  if (format === "date") {
    return toText(value);
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return toText(value);
  }
  if (format === "currency") {
    // `Decimal("-0.0") < 0` is false, so a negative zero prints unsigned here
    // and signed under `percent`, which formats the Decimal itself.
    return `${value < 0 ? "-" : ""}${currencyPrefix(currency)}${magnitude(value, 2)}`;
  }
  if (format === "integer") {
    // `int(Decimal("-0"))` is `0`: a value that rounds away to nothing loses
    // its sign.
    const rounded = magnitude(value, 0);
    return rounded === "0" ? rounded : `${value < 0 ? "-" : ""}${rounded}`;
  }
  if (format === "percent") {
    return `${isNegative(value) ? "-" : ""}${magnitude(value, 1)}%`;
  }
  // `number` prints an integral value through `int()`, which drops a negative
  // zero, and everything else through the Decimal, which keeps its sign.
  return Number.isInteger(value)
    ? `${value < 0 ? "-" : ""}${magnitude(value, 0)}`
    : `${isNegative(value) ? "-" : ""}${magnitude(value, 2)}`;
}
