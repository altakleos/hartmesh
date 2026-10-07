import { describe, expect, it } from "@rstest/core";

import {
  selectCustomerAdministration,
  type FeaturesResponse,
} from "@/core/features/api";

describe("effective customer administration", () => {
  it("denies omitted or malformed flags", () => {
    expect(
      selectCustomerAdministration({ agents_api: { enabled: true } }),
    ).toEqual({
      pluginManagement: false,
      localSkillManagement: false,
      localMcpManagement: false,
      providerOperations: false,
    });
    expect(
      selectCustomerAdministration({
        customer_administration: { local_skill_management: "true" },
      } as unknown as FeaturesResponse).localSkillManagement,
    ).toBe(false);
  });
  it("accepts only explicit effective permissions", () => {
    expect(
      selectCustomerAdministration({
        agents_api: { enabled: true },
        customer_administration: {
          local_skill_management: true,
          plugin_management: false,
          local_mcp_management: true,
        },
      }),
    ).toEqual({
      pluginManagement: false,
      localSkillManagement: true,
      localMcpManagement: true,
      providerOperations: false,
    });
  });
});
