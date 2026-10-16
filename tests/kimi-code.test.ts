import assert from "node:assert/strict";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";
import { createJiti } from "jiti";
import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import type { PiModel } from "../src/common.ts";
import { buildFixSuggestion, buildCompatDiagnosis, buildDoctorDiagnosis, describeRouterChannelDiagnostics, isCompatCheckApplicable } from "../src/diagnostics.ts";
import { describeMissingCacheCompatForModel, describeMissingOpenAICompatibleProxyCompat } from "../src/compat-advice.ts";
import { isOfficialKimiCodeEndpoint, isThirdPartyOpenAIProxyEndpoint } from "../src/model-identity.ts";
import { isKimiCodeV1BaseUrlChange, kimiCodeV1BaseUrl, parseOfficialKimiCodeBaseUrl } from "../src/kimi-code.ts";
import { buildKimiBaseUrlDoctorLines, diagnoseKimiCodeBaseUrl, planKimiBaseUrlFix } from "../src/kimi-base-url-fix.ts";
import { composeModelsJsonReceiptRollback, createBaseUrlFixReceipt, parseModelsJsonFixReceipt, validateModelsJsonRollback } from "../src/models-json-fix.ts";
import { parseJsonc } from "../src/jsonc.ts";


/** Pi 1.3.0's own kimi-coding catalog (openai-completions at /coding/v1). */
function builtInKimiModels(): PiModel[] {
  const dataPath = join(process.cwd(), "node_modules", "@earendil-works", "pi-ai", "dist", "providers", "data", "kimi-coding.json");
  const data = JSON.parse(readFileSync(dataPath, "utf8")) as Record<string, Record<string, Record<string, unknown>>>;
  return Object.values(data["openai-completions"] ?? {})
    .filter((entry) => entry.type === "chat")
    .map((entry) => entry as unknown as PiModel);
}

function kimi(overrides: Record<string, unknown> = {}): PiModel {
  return {
    provider: "kimi-coding", id: "k3", name: "Kimi K3", api: "openai-completions",
    baseUrl: "https://api.kimi.com/coding/v1", reasoning: true, input: ["text"],
    cost: { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 0 }, contextWindow: 1_048_576, maxTokens: 131_072,
    compat: { supportsStore: false, supportsDeveloperRole: false, thinkingFormat: "openai", requiresReasoningContentOnAssistantMessages: true },
    ...overrides,
  } as unknown as PiModel;
}

describe("official Kimi Code endpoint detection", () => {
  test("accepts only the official hosts with /coding or /coding/v1", () => {
    assert.deepEqual(parseOfficialKimiCodeBaseUrl("https://api.kimi.com/coding"), { host: "api.kimi.com", hasV1: false });
    assert.deepEqual(parseOfficialKimiCodeBaseUrl("https://api.kimi.ai/coding/v1/"), { host: "api.kimi.ai", hasV1: true });
    assert.deepEqual(parseOfficialKimiCodeBaseUrl(" https://API.KIMI.COM/coding/ "), { host: "api.kimi.com", hasV1: false });
    for (const url of [
      "http://api.kimi.com/coding",
      "https://api.kimi.com:8443/coding",
      "https://user:pw@api.kimi.com/coding",
      "https://api.kimi.com/coding?x=1",
      "https://api.kimi.com/coding/v2",
      "https://api.kimi.com/v1",
      "https://kimi-proxy.example.com/coding",
      "https://api.kimi.com.evil.example/coding",
      "https://api.moonshot.cn/v1",
      "",
    ]) {
      assert.equal(parseOfficialKimiCodeBaseUrl(url), undefined, url);
    }
  });

  test("appends /v1 only to a bare official /coding URL", () => {
    assert.equal(kimiCodeV1BaseUrl("https://api.kimi.com/coding"), "https://api.kimi.com/coding/v1");
    assert.equal(kimiCodeV1BaseUrl("https://api.kimi.ai/coding//"), "https://api.kimi.ai/coding/v1");
    assert.equal(kimiCodeV1BaseUrl("https://api.kimi.com/coding/v1"), undefined);
    assert.equal(kimiCodeV1BaseUrl("https://proxy.example.com/coding"), undefined);
    assert.equal(isKimiCodeV1BaseUrlChange("https://api.kimi.com/coding", "https://api.kimi.com/coding/v1"), true);
    assert.equal(isKimiCodeV1BaseUrlChange("https://api.kimi.com/coding", "https://evil.example/coding/v1"), false);
  });
});

describe("session-affinity advice skips the official Kimi Code origin", () => {
  test("Pi 1.3.0 built-in Kimi models get no proxy compat warning or fix", () => {
    const models = builtInKimiModels();
    assert.ok(models.length >= 3, "expected Pi's kimi-coding catalog");
    for (const model of models) {
      assert.equal(model.api, "openai-completions");
      assert.ok(isOfficialKimiCodeEndpoint(model));
      assert.equal(isThirdPartyOpenAIProxyEndpoint(model), false);
      assert.deepEqual(describeMissingOpenAICompatibleProxyCompat(model), [], model.id);
      assert.ok(!describeMissingCacheCompatForModel(model).includes("sendSessionAffinityHeaders"), model.id);
      assert.equal(buildFixSuggestion(model), undefined, model.id);
      assert.deepEqual(describeRouterChannelDiagnostics(model), [], model.id);
      assert.equal(isCompatCheckApplicable(model), false);
      const doctor = buildDoctorDiagnosis(model, {});
      assert.doesNotMatch(doctor, /Missing compat flags/, model.id);
      assert.match(doctor, /Official Kimi Code endpoint/);
      assert.equal(buildCompatDiagnosis(model), undefined, model.id);
    }
  });

  test("the international host and a /coding override are exempt too", () => {
    assert.deepEqual(describeMissingOpenAICompatibleProxyCompat(kimi({ baseUrl: "https://api.kimi.ai/coding/v1" })), []);
    assert.deepEqual(describeMissingOpenAICompatibleProxyCompat(kimi({ baseUrl: "https://api.kimi.com/coding" })), []);
  });

  test("Kimi through a third-party relay is still flagged", () => {
    const relayed = kimi({ provider: "relay", baseUrl: "https://relay.example.com/v1" });
    assert.deepEqual(describeMissingOpenAICompatibleProxyCompat(relayed), ["sendSessionAffinityHeaders"]);
    assert.deepEqual(buildFixSuggestion(relayed)?.compatKeys, { sendSessionAffinityHeaders: true });
    const mirrored = kimi({ provider: "kimi-coding", baseUrl: "https://kimi-mirror.example.com/coding/v1" });
    assert.deepEqual(describeMissingOpenAICompatibleProxyCompat(mirrored), ["sendSessionAffinityHeaders"]);
  });
});

describe("Kimi Code base URL diagnosis", () => {
  test("official /coding on OpenAI Chat Completions is fixable on both hosts", () => {
    assert.deepEqual(diagnoseKimiCodeBaseUrl(kimi({ baseUrl: "https://api.kimi.com/coding" })), {
      kind: "fixable", current: "https://api.kimi.com/coding", suggested: "https://api.kimi.com/coding/v1",
    });
    assert.deepEqual(diagnoseKimiCodeBaseUrl(kimi({ baseUrl: "https://api.kimi.ai/coding/" })), {
      kind: "fixable", current: "https://api.kimi.ai/coding/", suggested: "https://api.kimi.ai/coding/v1",
    });
    assert.match(buildKimiBaseUrlDoctorLines(kimi({ baseUrl: "https://api.kimi.com/coding" })).join("\n"), /fix kimi-base-url/);
  });

  test("an existing /v1 needs nothing", () => {
    assert.deepEqual(diagnoseKimiCodeBaseUrl(kimi()), { kind: "not-applicable" });
    assert.deepEqual(buildKimiBaseUrlDoctorLines(kimi()), []);
  });

  test("a proxy /coding path only gets a hint", () => {
    const status = diagnoseKimiCodeBaseUrl(kimi({ baseUrl: "https://kimi-proxy.example.com/coding" }));
    assert.equal(status.kind, "hint");
  });

  test("Anthropic Messages at /coding is left alone; /coding/v1 there only gets a hint", () => {
    assert.deepEqual(diagnoseKimiCodeBaseUrl(kimi({ api: "anthropic-messages", baseUrl: "https://api.kimi.com/coding" })), { kind: "not-applicable" });
    assert.equal(diagnoseKimiCodeBaseUrl(kimi({ api: "anthropic-messages", baseUrl: "https://api.kimi.com/coding/v1" })).kind, "hint");
  });
});

const PROVIDER_LEVEL = `{
  // Anthropic-era override
  "providers": {
    "kimi-coding": {
      "baseUrl": "https://api.kimi.com/coding", // old endpoint
      "apiKey": "KIMI_API_KEY"
    },
    "other": { "baseUrl": "https://api.kimi.com/coding", "api": "openai-completions", "models": [{ "id": "x" }] }
  }
}
`;

describe("planKimiBaseUrlFix", () => {
  const model = kimi({ baseUrl: "https://api.kimi.com/coding" });

  test("rewrites only the provider baseUrl and keeps comments", () => {
    const plan = planKimiBaseUrlFix(PROVIDER_LEVEL, model);
    assert.ok(!("error" in plan), JSON.stringify(plan));
    assert.equal(plan.placement, "provider");
    assert.equal(plan.after, "https://api.kimi.com/coding/v1");
    assert.equal(plan.modifiedText, PROVIDER_LEVEL.replace('"https://api.kimi.com/coding", // old', '"https://api.kimi.com/coding/v1", // old'));
    const parsed = parseJsonc(plan.modifiedText) as { providers: Record<string, { baseUrl: string }> };
    assert.equal(parsed.providers.other.baseUrl, "https://api.kimi.com/coding");
  });

  test("prefers the model's own custom baseUrl", () => {
    const text = `{"providers":{"kimi-coding":{"baseUrl":"https://api.kimi.com/coding/v1","models":[{"id":"k3","baseUrl":"https://api.kimi.ai/coding"}]}}}`;
    const plan = planKimiBaseUrlFix(text, kimi({ baseUrl: "https://api.kimi.ai/coding" }));
    assert.ok(!("error" in plan), JSON.stringify(plan));
    assert.equal(plan.placement, "model");
    assert.deepEqual(parseJsonc(plan.modifiedText), {
      providers: { "kimi-coding": { baseUrl: "https://api.kimi.com/coding/v1", models: [{ id: "k3", baseUrl: "https://api.kimi.ai/coding/v1" }] } },
    });
  });

  test("refuses when the live base URL comes from somewhere else", () => {
    const text = `{"providers":{"kimi-coding":{"baseUrl":"https://api.kimi.ai/coding"}}}`;
    assert.ok("error" in planKimiBaseUrlFix(text, model));
    assert.ok("error" in planKimiBaseUrlFix(`{"providers":{}}`, model));
  });

  test("refuses a provider baseUrl shared with an Anthropic custom model", () => {
    const text = `{"providers":{"kimi-coding":{"baseUrl":"https://api.kimi.com/coding","models":[{"id":"legacy","api":"anthropic-messages"}]}}}`;
    const plan = planKimiBaseUrlFix(text, model);
    assert.ok("error" in plan && /legacy/.test(plan.error));
  });

  test("refuses a provider baseUrl that also feeds built-in models on another API", () => {
    const text = `{"providers":{"kimi-coding":{"baseUrl":"https://api.kimi.com/coding","models":[{"id":"custom-oa","api":"openai-completions"}]}}}`;
    const custom = kimi({ id: "custom-oa", baseUrl: "https://api.kimi.com/coding" });
    const legacyBuiltIn = kimi({ id: "kimi-for-coding", api: "anthropic-messages", baseUrl: "https://api.kimi.com/coding" });
    const withRegistry = planKimiBaseUrlFix(text, custom, [custom, legacyBuiltIn]);
    assert.ok("error" in withRegistry && /kimi-for-coding/.test(withRegistry.error));
    // No registry: a custom entry inheriting the provider baseUrl is refused.
    assert.ok("error" in planKimiBaseUrlFix(text, custom));
    // Pi 1.3.0 registry (all built-ins on openai-completions) allows it.
    const ok = planKimiBaseUrlFix(text, custom, [custom, kimi({ id: "kimi-for-coding", baseUrl: "https://api.kimi.com/coding" })]);
    assert.ok(!("error" in ok), JSON.stringify(ok));
  });

  test("refuses models that do not need the fix", () => {
    assert.ok("error" in planKimiBaseUrlFix(PROVIDER_LEVEL, kimi()));
    assert.ok("error" in planKimiBaseUrlFix(PROVIDER_LEVEL, kimi({ api: "anthropic-messages", baseUrl: "https://api.kimi.com/coding" })));
    assert.ok("error" in planKimiBaseUrlFix(`{"providers":{"kimi-coding":{"baseUrl":"https://proxy.example.com/coding"}}}`, kimi({ baseUrl: "https://proxy.example.com/coding" })));
  });
});

test("official Kimi doctor omits the passive proxy SDK-header 403 hint", () => {
  const text = buildDoctorDiagnosis(kimi());
  assert.doesNotMatch(text, /after disabling sendSessionAffinityHeaders/);
  assert.match(buildDoctorDiagnosis(kimi(), { openAISdkHeader403: true }), /403 response was observed/);
  assert.match(buildDoctorDiagnosis(kimi({ provider: "relay", baseUrl: "https://relay.example.com/coding/v1" })), /after disabling sendSessionAffinityHeaders/);
});

describe("version 3 base URL receipts", () => {
  const model = kimi({ baseUrl: "https://api.kimi.com/coding" });
  const plan = planKimiBaseUrlFix(PROVIDER_LEVEL, model);
  assert.ok(!("error" in plan));
  const receipt = createBaseUrlFixReceipt(PROVIDER_LEVEL, plan.modifiedText, "kimi-coding", "k3", plan.placement, plan.before, plan.after, "/tmp/models.json.backup-cache-optimizer-1");

  test("round-trips and rejects tampering", () => {
    assert.ok(receipt);
    assert.equal(receipt.version, 3);
    assert.deepEqual(parseModelsJsonFixReceipt(JSON.parse(JSON.stringify(receipt))), receipt);
    assert.equal(parseModelsJsonFixReceipt({ ...receipt, baseUrlChange: { before: plan.before, after: "https://evil.example/coding/v1" } }), undefined);
    assert.equal(parseModelsJsonFixReceipt({ ...receipt, version: 2 }), undefined);
    assert.equal(parseModelsJsonFixReceipt({ ...receipt, placement: "modelOverride" }), undefined);
    assert.equal(parseModelsJsonFixReceipt({ ...receipt, changedKeys: { sendSessionAffinityHeaders: { before: { present: false }, after: { present: true, value: true } } } }), undefined);
  });

  test("surgical rollback restores only baseUrl after unrelated edits", () => {
    assert.ok(receipt);
    const edited = plan.modifiedText.replace('"apiKey": "KIMI_API_KEY"', '"apiKey": "KIMI_API_KEY",\n      "headers": { "x": "y" }');
    const rollback = composeModelsJsonReceiptRollback(edited, receipt);
    assert.ok(!("error" in rollback));
    const parsed = parseJsonc(rollback.modifiedText) as { providers: Record<string, Record<string, unknown>> };
    assert.equal(parsed.providers["kimi-coding"].baseUrl, "https://api.kimi.com/coding");
    assert.deepEqual(parsed.providers["kimi-coding"].headers, { x: "y" });
    assert.equal(validateModelsJsonRollback(rollback.modifiedText, receipt), null);
  });

  test("refuses when the user changed the base URL afterwards", () => {
    assert.ok(receipt);
    const edited = plan.modifiedText.replace("https://api.kimi.com/coding/v1", "https://api.kimi.ai/coding/v1");
    assert.ok("error" in composeModelsJsonReceiptRollback(edited, receipt));
  });
});

describe("/cache-optimizer fix kimi-base-url end to end", () => {
  test("plain fix repairs the base URL with backup and receipt, and rollback restores it", async () => {
    const dir = await mkdtemp(join(tmpdir(), "pi-cache-kimi-base-url-"));
    const saved = process.env.PI_CODING_AGENT_DIR;
    process.env.PI_CODING_AGENT_DIR = dir;
    try {
      await writeFile(join(dir, "models.json"), PROVIDER_LEVEL);
      const jiti = createJiti(join(process.cwd(), "tests", "kimi-code.test.ts"), { interopDefault: false, moduleCache: false });
      const loaded = await jiti.import<typeof import("../index.ts")>(join(process.cwd(), "index.ts"));
      const commands = new Map<string, { handler: (args: string, ctx: ExtensionCommandContext) => Promise<void> }>();
      loaded.default({
        on() {},
        registerCommand(name: string, command: { handler: (args: string, ctx: ExtensionCommandContext) => Promise<void> }) { commands.set(name, command); },
      } as unknown as ExtensionAPI);
      const notices: string[] = [];
      const previews: string[] = [];
      const ctx = {
        model: kimi({ baseUrl: "https://api.kimi.com/coding" }), mode: "interactive", hasUI: true,
        sessionManager: { getSessionId: () => "s", getBranch: () => [] },
        modelRegistry: { find: () => undefined, getAvailable: () => [], getAll: () => [] },
        ui: {
          notify: (text: string) => notices.push(text),
          setStatus: () => {},
          confirm: async (_title: string, message: string) => { previews.push(message); return true; },
        },
      } as unknown as ExtensionCommandContext;
      const run = (args: string) => commands.get("cache-optimizer")!.handler(args, ctx);

      await run("fix");
      assert.match(previews[0] ?? "", /coding\/v1/, notices.join("\n"));
      const written = await readFile(join(dir, "models.json"), "utf8");
      assert.equal(written, PROVIDER_LEVEL.replace('"https://api.kimi.com/coding", // old', '"https://api.kimi.com/coding/v1", // old'));
      assert.ok((await readdir(dir)).some((name) => name.startsWith("models.json.backup-cache-optimizer-")));
      const receipt = JSON.parse(await readFile(join(dir, "pi-cache-optimizer-fix-receipt.json"), "utf8"));
      assert.equal(receipt.version, 3);

      await run("rollback");
      assert.equal(await readFile(join(dir, "models.json"), "utf8"), PROVIDER_LEVEL, notices.join("\n"));
    } finally {
      if (saved === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = saved;
      await rm(dir, { recursive: true, force: true });
    }
  });
});
