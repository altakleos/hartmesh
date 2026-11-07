import "katex/dist/katex.min.css";
import "streamdown/styles.css";

import { type Metadata } from "next";
import { redirect } from "next/navigation";

import { RedirectToLogin } from "@/components/workspace/redirect-to-login";
import { WorkspaceAuthProvider } from "@/components/workspace/workspace-auth-provider";
import { getServerSideUser } from "@/core/auth/server";
import { assertNever } from "@/core/auth/types";
import { detectLocaleServer } from "@/core/i18n/server";
import { RuntimeProductProvider } from "@/core/product/context";
import {
  getServerSideProductIdentity,
  productMetadata,
} from "@/core/product/server";

import { WorkspaceContent } from "./workspace-content";

export const dynamic = "force-dynamic";

/** The tab title until a page sets its own: the deployment's product name. */
export async function generateMetadata(): Promise<Metadata> {
  return productMetadata();
}

export default async function WorkspaceLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const [locale, result, snapshot] = await Promise.all([
    detectLocaleServer(),
    getServerSideUser(),
    getServerSideProductIdentity(),
  ]);

  let content: React.ReactNode;

  switch (result.tag) {
    case "authenticated":
      content = (
        <WorkspaceAuthProvider verifiedUser={result.user}>
          <WorkspaceContent>{children}</WorkspaceContent>
        </WorkspaceAuthProvider>
      );
      break;
    case "needs_setup":
      redirect("/setup");
    case "system_setup_required":
      redirect("/setup");
    case "unauthenticated":
      content = <RedirectToLogin />;
      break;
    case "gateway_unavailable":
      // Keep the same provider path as authenticated refreshes. Uncertain auth
      // must not retire an already admitted account/document presentation.
      content = (
        <WorkspaceAuthProvider>
          <WorkspaceContent gatewayUnavailable>{children}</WorkspaceContent>
        </WorkspaceAuthProvider>
      );
      break;
    case "config_error":
      throw new Error(result.message);
    default:
      assertNever(result);
  }

  return (
    <RuntimeProductProvider initialLocale={locale} snapshot={snapshot}>
      {content}
    </RuntimeProductProvider>
  );
}
