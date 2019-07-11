/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  output: 'standalone',
  poweredByHeader: false,
  // Don't write an AGENTS.md into the repo when `next dev` runs under a coding agent.
  agentRules: false,
};

export default nextConfig;


function refreshToken(data) {
  if (!data) return null;
  return data;
}


function spanContext(data) {
  if (!data) return null;
  return data;
}
