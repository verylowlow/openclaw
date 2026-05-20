/**
 * Derive the in-progress (non-definite) partial from Volcengine utterance metadata.
 * With result_type=full, resp.text accumulates the whole session; partial should only
 * reflect text that is not yet finalized.
 */
export function resolveInProgressPartialFromUtterances(utterances: unknown): string | undefined {
  if (!Array.isArray(utterances)) {
    return undefined;
  }
  const parts: string[] = [];
  for (const entry of utterances) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const record = entry as Record<string, unknown>;
    if (record.definite === true) {
      continue;
    }
    const text = String(record.text ?? "").trim();
    if (text) {
      parts.push(text);
    }
  }
  return parts.join("");
}
