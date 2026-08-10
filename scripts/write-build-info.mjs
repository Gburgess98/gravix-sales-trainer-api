/**
 * write-build-info.mjs — Day 278
 *
 * Stamps the ACTUAL git commit into src/build-info.json at deploy time, so the
 * deployed API's /v1/version reports the commit that was really shipped.
 *
 * Why this exists: the staging API is deployed with `railway up` (a tarball of the
 * working directory). Railway does NOT populate RAILWAY_GIT_COMMIT_SHA for CLI
 * uploads, and .railwayignore excludes .git from the build, so the builder can't
 * read the commit either. This script runs LOCALLY (git available) right before
 * `railway up`, writing the short SHA into a file that IS uploaded and read at
 * runtime. No SHA is hard-coded; nothing secret is written.
 *
 * Used by `npm run deploy:staging`. Safe to run anytime; if git is unavailable it
 * writes commit:null and /v1/version falls back to env vars.
 */
import { writeFileSync } from "node:fs";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const out = join(__dirname, "..", "src", "build-info.json");

function git(cmd) {
  try {
    return execSync(`git ${cmd}`, { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

const commit = git("rev-parse --short HEAD") || null;
// Working-tree dirtiness, ignoring the build-info file we are about to write.
const dirty =
  git("status --porcelain")
    .split("\n")
    .filter((l) => l && !l.includes("src/build-info.json")).length > 0;

const info = { commit, dirty, builtAt: new Date().toISOString() };
writeFileSync(out, JSON.stringify(info, null, 2) + "\n");
console.log("wrote src/build-info.json:", JSON.stringify(info));
