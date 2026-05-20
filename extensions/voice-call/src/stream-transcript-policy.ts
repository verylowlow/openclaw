/** Minimum length for a standalone auto-response (excludes "哦。" etc.). */
export const STREAM_MIN_AUTO_RESPONSE_CHARS = 3;

const FILLER_ONLY = /^(?:哦|啊|嗯|呃|哈|呀|呢|吧|噢|喔|诶|欸)+[。！？，,.!?…~\s]*$/u;

const GOODBYE_INTENT = /(?:再见|拜拜|没别的事|就这样吧|没有啦|没有了|下一个|先这样|挂了|不打扰)/u;

/**
 * Skip very short filler-only definite utterances that should not start a new LLM turn.
 */
export function shouldSkipFillerTranscript(transcript: string): boolean {
  const trimmed = transcript.trim();
  if (!trimmed) {
    return true;
  }
  if (trimmed.length < STREAM_MIN_AUTO_RESPONSE_CHARS) {
    return true;
  }
  if (FILLER_ONLY.test(trimmed)) {
    return true;
  }
  return false;
}

export function looksLikeGoodbyeIntent(transcript: string): boolean {
  return GOODBYE_INTENT.test(transcript.trim());
}

/**
 * Skip when a new definite is already covered by the last agent turn (substring overlap).
 */
/**
 * Merge rapid consecutive definite utterances within one debounce window.
 * Prefer the longer text when one extends the other; otherwise join with a space.
 */
export function mergeStreamTranscripts(previous: string, next: string): string {
  const prev = previous.trim();
  const nxt = next.trim();
  if (!prev) {
    return nxt;
  }
  if (!nxt) {
    return prev;
  }
  if (nxt.startsWith(prev)) {
    return nxt;
  }
  if (prev.startsWith(nxt)) {
    return prev;
  }
  return `${prev} ${nxt}`;
}

export function isRedundantFollowUpTranscript(
  nextTranscript: string,
  lastRespondedTranscript: string,
): boolean {
  const next = nextTranscript.trim();
  const last = lastRespondedTranscript.trim();
  if (!next || !last) {
    return false;
  }
  if (next === last) {
    return true;
  }
  if (last.includes(next) && next.length < last.length) {
    return true;
  }
  return false;
}
