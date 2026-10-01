import { redirect } from "next/navigation";

/**
 * The product is the workspace: `/` goes straight there, and the workspace
 * sends someone who is not signed in to the sign-in page (or setup).
 */
export default function RootPage() {
  redirect("/workspace");
}
