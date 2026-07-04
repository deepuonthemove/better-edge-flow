export function parseDuration(duration: string | number): number {
  if (typeof duration === "number") {
    return duration;
  }
  const match = duration.trim().match(/^(\d+)\s*([smhd])$/i);
  if (!match) {
    throw new Error(`Invalid duration format: "${duration}". Expected format like "10s", "15m", "24h", or "3d".`);
  }
  const value = parseInt(match[1], 10);
  const unit = match[2].toLowerCase();
  switch (unit) {
    case "s":
      return value * 1000;
    case "m":
      return value * 60 * 1000;
    case "h":
      return value * 60 * 60 * 1000;
    case "d":
      return value * 24 * 60 * 60 * 1000;
    default:
      throw new Error(`Unsupported duration unit: "${unit}"`);
  }
}
