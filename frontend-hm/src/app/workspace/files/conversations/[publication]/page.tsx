"use client";
import { useParams } from "next/navigation";

import { SnapshotReader } from "@/components/workspace/shared-conversations/reader";
export default function SharedConversationPage() {
  const { publication } = useParams<{ publication: string }>();
  return <SnapshotReader key={publication} id={publication} />;
}
