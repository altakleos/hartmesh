export interface MCPServerConfig extends Record<string, unknown> {
  enabled: boolean;
  description: string;
}

export interface MCPConfig {
  given_agent_counts?: Record<string, number>;
  mcp_servers: Record<string, MCPServerConfig>;
}
