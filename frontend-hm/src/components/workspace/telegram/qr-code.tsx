"use client";

import { QRCodeSVG } from "qrcode.react";

/** Drawn in the browser: the second code holds a connect code, which no request may carry. */
export function TelegramQrCode({
  value,
  label,
}: {
  value: string;
  label: string;
}) {
  return (
    <figure className="flex flex-col items-center gap-2">
      <QRCodeSVG
        value={value}
        size={176}
        marginSize={4}
        bgColor="#FFFFFF"
        role="img"
        aria-label={label}
      />
      <figcaption className="text-muted-foreground text-center text-xs">
        {label}
      </figcaption>
    </figure>
  );
}
