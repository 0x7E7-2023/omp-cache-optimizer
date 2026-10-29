import { type PiModel, asRecord, isNonEmptyString } from "./common.ts";
import { parseJsonc } from "./jsonc.ts";
import { kimiCodeV1BaseUrl, normalizeBaseUrlForComparison, parseOfficialKimiCodeBaseUrl } from "./kimi-code.ts";
import { isValidModelsConfigForEffectiveCompat } from "./model-identity.ts";
import { type BaseUrlFixPlacement, locateBaseUrlTarget, readBaseUrlState, replaceBaseUrlValue } from "./models-json-fix.ts";

/**
 * Kimi Code base URL check and `/cache-optimizer fix kimi-base-url`.
 *
 * Pi 1.3.0 moved the built-in kimi-coding provider from anthropic-messages
 * (`https://api.kimi.com/coding`) to OpenAI Chat Completions
 * (`https://api.kimi.com/coding/v1`). A models.json `baseUrl` left over from
 * the Anthropic era keeps `/coding`, so Pi posts to `/coding/chat/completions`
 * and every request fails.
 *
 * The fix only appends `/v1`, and only when the live model uses
 * openai-completions and its base URL is exactly an official Kimi Code host
 * with the bare `/coding` path. The effective API is the real condition: Pi
 * versions before 1.3.0 ship kimi-coding as anthropic-messages, and Kimi's
 * OpenAI-compatible endpoint has always been `/coding/v1`. Proxies, other
 * paths, and Anthropic-protocol entries only get a hint.
 */

export type KimiBaseUrlStatus =
  | { kind: "not-applicable" }
  | { kind: "fixable"; current: string; suggested: string }
  | { kind: "hint"; lines: string[] };

const KIMI_CODING_PATH = /\/coding\/*$/i;

export function diagnoseKimiCodeBaseUrl(model: PiModel | undefined): KimiBaseUrlStatus {
  if (!model || !isNonEmptyString(model.baseUrl)) return { kind: "not-applicable" };
  const api = String(model.api ?? "").toLowerCase();
  const official = parseOfficialKimiCodeBaseUrl(model.baseUrl);

  if (api === "openai-completions") {
    if (official && !official.hasV1) {
      const suggested = kimiCodeV1BaseUrl(model.baseUrl);
      if (suggested) return { kind: "fixable", current: model.baseUrl, suggested };
    }
    if (!official && isKimiLike(model) && KIMI_CODING_PATH.test(pathOf(model.baseUrl))) {
      return {
        kind: "hint",
        lines: [
          "ℹ️ This Kimi entry uses OpenAI Chat Completions, and its base URL ends in /coding without /v1.",
          "   Kimi Code's official OpenAI endpoint is https://api.kimi.com/coding/v1. Check what your proxy expects;",
          "   it is not an official Kimi host, so /cache-optimizer fix leaves it unchanged.",
        ],
      };
    }
    return { kind: "not-applicable" };
  }

  if (api === "anthropic-messages" && official?.hasV1) {
    return {
      kind: "hint",
      lines: [
        "ℹ️ This Kimi Code entry uses the Anthropic Messages API, but its base URL ends in /coding/v1.",
        "   The Anthropic endpoint is https://api.kimi.com/coding (Pi appends /v1/messages itself).",
        "   Pi 1.3.0+ ships kimi-coding on OpenAI Chat Completions; drop the api override or the /v1. Not changed automatically.",
      ],
    };
  }
  return { kind: "not-applicable" };
}

function isKimiLike(model: PiModel): boolean {
  const provider = String(model.provider ?? "").toLowerCase();
  const baseUrl = String(model.baseUrl ?? "").toLowerCase();
  return provider.includes("kimi") || provider.includes("moonshot") || baseUrl.includes("kimi") || baseUrl.includes("moonshot");
}

function pathOf(baseUrl: string): string {
  try {
    return new URL(baseUrl.trim()).pathname;
  } catch {
    return "";
  }
}

export function buildKimiBaseUrlDoctorLines(model: PiModel | undefined): string[] {
  const status = diagnoseKimiCodeBaseUrl(model);
  if (status.kind === "fixable") {
    return [
      "",
      "⚠️  Kimi Code base URL is missing /v1.",
      `   Current:  ${status.current}`,
      `   Expected: ${status.suggested}`,
      "   This model uses OpenAI Chat Completions (Pi 1.3.0+ kimi-coding), so requests to /coding/chat/completions fail.",
      "   Run /cache-optimizer fix kimi-base-url to append /v1 with a preview, backup and rollback.",
    ];
  }
  if (status.kind === "hint") return ["", ...status.lines];
  return [];
}

export type KimiBaseUrlFixPlan = {
  modifiedText: string;
  placement: BaseUrlFixPlacement;
  before: string;
  after: string;
  locationLabel: string;
  /** Other models whose base URL changes too (provider-level edits). */
  affectedNote?: string;
};

/**
 * Plan the models.json edit for the active model. The `baseUrl` Pi uses comes
 * from the model's own custom `models[]` entry when it has one, otherwise
 * from the provider entry. The located string must match the live base URL;
 * a mismatch means something else (an extension, another file) supplies it.
 */
export function planKimiBaseUrlFix(
  originalText: string,
  model: PiModel,
  /** Live models from Pi's registry (`modelRegistry.getAll()`), when available. */
  registeredModels?: readonly PiModel[],
): KimiBaseUrlFixPlan | { error: string } {
  const status = diagnoseKimiCodeBaseUrl(model);
  if (status.kind !== "fixable") return { error: "the active model does not need the Kimi Code /v1 fix" };

  let parsed: unknown;
  try {
    parsed = parseJsonc(originalText);
  } catch {
    return { error: "models.json is not valid JSONC" };
  }
  if (!isValidModelsConfigForEffectiveCompat(parsed)) {
    return { error: "models.json does not match Pi's model configuration schema" };
  }
  const providerRecord = asRecord(asRecord(asRecord(parsed)?.providers)?.[model.provider]);
  if (!providerRecord) {
    return { error: `models.json has no "${model.provider}" provider entry, so the base URL comes from somewhere else (an extension or Pi's built-in catalog)` };
  }
  const definitions = (Array.isArray(providerRecord.models) ? providerRecord.models : []).map(asRecord);
  const ownDefinitions = definitions.filter((entry) => entry?.id === model.id);
  if (ownDefinitions.length > 1) return { error: `models.json defines ${model.provider}/${model.id} more than once` };

  let placement: BaseUrlFixPlacement;
  if (ownDefinitions[0] && ownDefinitions[0].baseUrl !== undefined) {
    placement = "model";
  } else if (providerRecord.baseUrl !== undefined) {
    placement = "provider";
    if (providerRecord.oauth === "radius") {
      return { error: "the provider uses oauth \"radius\", where Pi ignores the provider baseUrl for built-in models" };
    }
    // A provider-level baseUrl also feeds custom models without their own.
    // Refuse when one of them is declared on another protocol, since /v1
    // would break it (Anthropic Messages wants the bare /coding path).
    const providerApi = providerRecord.api;
    const conflicting = definitions
      .filter((entry) => entry && entry.baseUrl === undefined)
      .filter((entry) => {
        const api = entry?.api ?? providerApi;
        return api !== undefined && String(api).toLowerCase() !== "openai-completions";
      })
      .map((entry) => String(entry?.id));
    if (conflicting.length > 0) {
      return { error: `the provider baseUrl is shared with custom models on another API (${conflicting.join(", ")}); give each its own baseUrl first` };
    }
    // The provider baseUrl also overrides every built-in model of the
    // provider. On Pi < 1.3.0 the built-in kimi-coding models are
    // anthropic-messages, so a custom openai-completions entry inheriting the
    // provider baseUrl must not drag them onto /coding/v1. Check the live
    // registry; without it, refuse for a custom entry (built-ins unknown).
    const ownIds = new Set(definitions.map((entry) => entry?.id));
    if (registeredModels) {
      const builtInConflicts = registeredModels
        .filter((entry) => entry.provider === model.provider && !ownIds.has(entry.id))
        .filter((entry) => String(entry.api ?? "").toLowerCase() !== "openai-completions")
        .filter((entry) => isNonEmptyString(entry.baseUrl) && normalizeBaseUrlForComparison(entry.baseUrl) === normalizeBaseUrlForComparison(String(providerRecord.baseUrl)))
        .map((entry) => String(entry.id));
      if (builtInConflicts.length > 0) {
        return { error: `the provider baseUrl is shared with built-in models on another API (${builtInConflicts.join(", ")}); set baseUrl on the ${model.id} models[] entry instead` };
      }
    } else if (ownDefinitions[0]) {
      return { error: `the provider baseUrl may also feed built-in ${model.provider} models on another API; set baseUrl on the ${model.id} models[] entry instead` };
    }
  } else {
    return { error: `models.json sets no baseUrl for ${model.provider}/${model.id}, so the base URL comes from somewhere else` };
  }

  const target = locateBaseUrlTarget(originalText, model.provider, model.id, placement);
  const before = readBaseUrlState(originalText, target);
  if (!target || before === undefined || before === "invalid") {
    return { error: "the baseUrl entry is duplicated or cannot be located safely" };
  }
  if (normalizeBaseUrlForComparison(before) !== normalizeBaseUrlForComparison(status.current)) {
    return { error: "the baseUrl in models.json differs from the active model's base URL (an extension or another source overrides it)" };
  }
  const after = kimiCodeV1BaseUrl(before);
  if (!after) return { error: "the baseUrl in models.json is not a bare official Kimi Code /coding URL" };
  const modifiedText = replaceBaseUrlValue(originalText, target, after);
  if (modifiedText === undefined) return { error: "the baseUrl value cannot be rewritten safely" };

  const locationLabel = placement === "provider"
    ? `providers[${JSON.stringify(model.provider)}].baseUrl`
    : `providers[${JSON.stringify(model.provider)}].models[id=${JSON.stringify(model.id)}].baseUrl`;
  const plan: KimiBaseUrlFixPlan = {
    modifiedText,
    placement,
    before,
    after,
    locationLabel,
    ...(placement === "provider" ? { affectedNote: `Every ${model.provider} model without its own baseUrl uses this value.` } : {}),
  };
  const checkError = selfCheckKimiBaseUrlFix(originalText, plan, model.provider, model.id);
  if (checkError) return { error: `self-check failed: ${checkError}` };
  return plan;
}

/** The edit must be valid, write `after`, and change nothing but that string. */
export function selfCheckKimiBaseUrlFix(originalText: string, plan: KimiBaseUrlFixPlan, provider: string, modelId: string): string | null {
  let original: unknown;
  let modified: unknown;
  try {
    original = parseJsonc(originalText);
    modified = parseJsonc(plan.modifiedText);
  } catch {
    return "the edited file is not valid JSONC";
  }
  if (!isValidModelsConfigForEffectiveCompat(modified)) return "the edited file does not match Pi's model configuration schema";
  if (readBaseUrlState(plan.modifiedText, locateBaseUrlTarget(plan.modifiedText, provider, modelId, plan.placement)) !== plan.after) {
    return "baseUrl was not written at the target";
  }
  const reverted = JSON.parse(JSON.stringify(modified)) as Record<string, unknown>;
  const providerRecord = asRecord(asRecord(reverted.providers)?.[provider]);
  if (!providerRecord) return "the provider entry disappeared";
  if (plan.placement === "provider") {
    providerRecord.baseUrl = plan.before;
  } else {
    const entry = (Array.isArray(providerRecord.models) ? providerRecord.models : []).map(asRecord).find((item) => item?.id === modelId);
    if (!entry) return "the model entry disappeared";
    entry.baseUrl = plan.before;
  }
  if (JSON.stringify(reverted) !== JSON.stringify(original)) return "unrelated configuration was altered";
  return null;
}

/** Validate a file written by the fix transaction (re-read from disk). */
export function validateWrittenKimiBaseUrlFix(writtenText: string, plan: KimiBaseUrlFixPlan): string | null {
  if (writtenText !== plan.modifiedText) return "models.json content differs from the previewed edit";
  try {
    parseJsonc(writtenText);
  } catch {
    return "the written file is not valid JSONC";
  }
  return null;
}
