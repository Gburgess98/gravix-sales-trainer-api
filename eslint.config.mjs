// @ts-check
// Flat ESLint config for the Gravix Sales Trainer API.
// Scope: production source only (`src/**/*.ts`). Generated output, dependencies
// and coverage are ignored; no production source file is excluded.
import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    // Only generated artefacts, dependencies and coverage — never source.
    ignores: ["dist/**", "node_modules/**", "coverage/**", ".build/**"],
  },
  {
    files: ["src/**/*.ts"],
    extends: [js.configs.recommended, tseslint.configs.recommended],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
    },
    rules: {
      // TypeScript's compiler already resolves identifiers and types, so ESLint's
      // no-undef only double-reports Node/DOM globals here. Disabled per the
      // typescript-eslint guidance for TypeScript sources.
      "no-undef": "off",
      // Honour the codebase's deliberate leading-underscore convention for
      // intentionally-unused bindings (e.g. Express error-handler `_next`, whose
      // 4-arg arity is required at runtime; array-destructure skips; rest-omit
      // siblings). Genuinely dead non-underscore bindings still error.
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
          ignoreRestSiblings: true,
        },
      ],
      // Foundation batch only: the API carries ~1.8k pre-existing `any` usages.
      // Error-level enforcement would force an unsafe broad refactor, so this is
      // recorded as tracked warning debt (Go Live Day 4) rather than hidden.
      "@typescript-eslint/no-explicit-any": "warn",
    },
  }
);
