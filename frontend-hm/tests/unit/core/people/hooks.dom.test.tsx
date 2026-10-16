import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";

rs.mock("@/core/people/api", () => ({ getPeople: rs.fn() }));
const auth = rs.hoisted(() => ({ user: { id: "alice", system_role: "user" } }));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => auth }));
import { getPeople } from "@/core/people/api";
import { usePeople } from "@/core/people/hooks";

const clients: QueryClient[] = [];
function Probe({ ids, label = "people" }: { ids: string[]; label?: string }) {
  const people = usePeople(ids);
  return (
    <output data-testid={label}>
      {JSON.stringify(Object.fromEntries(people))}
    </output>
  );
}
function start(ids: string[]) {
  const client = new QueryClient();
  clients.push(client);
  const tree = (children = <Probe ids={ids} />) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return { view: render(tree()), tree };
}
function state(label = "people") {
  return JSON.parse(screen.getByTestId(label).textContent ?? "{}");
}
beforeEach(() => {
  rs.mocked(getPeople).mockReset();
  auth.user = { id: "alice", system_role: "user" };
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});

test("batches at most 50 unique ids and keeps missing people distinct from failed reads", async () => {
  rs.mocked(getPeople).mockImplementation(async (ids) => ({
    users: ids
      .filter((id) => id !== "missing")
      .map((id) => ({
        id,
        email: `${id}@example.com`,
        name: null,
        active: id !== "p0",
      })),
  }));
  const ids = [
    ...Array.from({ length: 52 }, (_, i) => `p${i}`),
    "missing",
    "p0",
  ];
  start(ids);
  await waitFor(() => expect(state().p51.status).toBe("ready"));
  expect(getPeople).toHaveBeenCalledTimes(2);
  expect(
    rs.mocked(getPeople).mock.calls.map(([batch]) => batch.length),
  ).toEqual([50, 3]);
  expect(state().missing).toEqual({ status: "ready", person: null });
  expect(state().p0.person.active).toBe(false);
});

test("shares in-flight and cached ids across components and remounts", async () => {
  rs.mocked(getPeople).mockResolvedValue({
    users: [{ id: "lee", email: "lee@example.com", name: null, active: true }],
  });
  const { view, tree } = start(["lee"]);
  await waitFor(() => expect(state().lee.status).toBe("ready"));
  view.rerender(
    tree(
      <>
        <Probe ids={["lee"]} />
        <Probe ids={["lee"]} label="second" />
      </>,
    ),
  );
  await waitFor(() =>
    expect(state("second").lee.person.email).toBe("lee@example.com"),
  );
  expect(getPeople).toHaveBeenCalledTimes(1);
});

test("does not turn lookup failures into former colleagues or retry on mount", async () => {
  rs.mocked(getPeople).mockRejectedValue(new Error("offline"));
  const { view, tree } = start(["lee"]);
  await waitFor(() => expect(state().lee.status).toBe("error"));
  view.rerender(tree(<Probe ids={["lee"]} label="second" />));
  await waitFor(() => expect(state("second").lee.status).toBe("error"));
  expect(getPeople).toHaveBeenCalledTimes(1);
});

test.each([
  { id: "bob", system_role: "user" },
  { id: "alice", system_role: "admin" },
])("fences directory data across identity changes to %j", async (next) => {
  let finish!: (value: Awaited<ReturnType<typeof getPeople>>) => void;
  let oldSignal: AbortSignal | undefined;
  rs.mocked(getPeople)
    .mockImplementationOnce((_ids, signal) => {
      oldSignal = signal;
      return new Promise((resolve) => {
        finish = resolve;
      });
    })
    .mockResolvedValue({ users: [] });
  const { view, tree } = start(["lee"]);
  await waitFor(() => expect(getPeople).toHaveBeenCalledTimes(1));
  auth.user = next;
  view.rerender(tree());
  expect(state().lee.person).toBeNull();
  await waitFor(() => expect(state().lee.status).toBe("ready"));
  expect(oldSignal?.aborted).toBe(true);
  await act(async () =>
    finish({
      users: [
        {
          id: "lee",
          email: "private-old@example.com",
          name: null,
          active: true,
        },
      ],
    }),
  );
  expect(state().lee.person).toBeNull();
});
