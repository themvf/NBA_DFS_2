import "server-only";

export function getOpenAIApiKey(): string | null {
  return process.env.OPENAI_API_KEY?.trim() || process.env.OPENAI_API?.trim() || null;
}

export function getBestBallAdvisorAvailability(): { openai: boolean; deepseek: boolean } {
  return {
    openai: Boolean(getOpenAIApiKey()),
    deepseek: Boolean(process.env.DEEPSEEK_API_KEY?.trim()),
  };
}
