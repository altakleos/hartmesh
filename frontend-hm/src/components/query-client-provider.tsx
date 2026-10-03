"use client";

import {
  QueryClient,
  QueryClientProvider as TanStackQueryClientProvider,
} from "@tanstack/react-query";
import { useEffect, useState } from "react";

export function QueryClientProvider({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  // The authenticated subtree remounts when its identity changes. Keeping
  // clients local also isolates separate layouts and server-rendered requests.
  const [queryClient] = useState(() => new QueryClient());
  useEffect(() => () => queryClient.clear(), [queryClient]);

  return (
    <TanStackQueryClientProvider client={queryClient}>
      {children}
    </TanStackQueryClientProvider>
  );
}
