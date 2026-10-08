import { PluginPage } from "@/components/workspace/plugin-page";

export default async function ExtensionPage({
  params,
  searchParams,
}: {
  params: Promise<{ namespace: string; surface_id: string }>;
  searchParams: Promise<{ space?: string | string[] }>;
}) {
  const { namespace, surface_id } = await params;
  const { space } = await searchParams;
  return (
    <PluginPage
      namespace={namespace}
      surfaceId={surface_id}
      resourceId={typeof space === "string" ? space : undefined}
    />
  );
}
