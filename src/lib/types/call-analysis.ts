export type StageScore = {
  score: number;
  notes: string;
};

export type CallMomentType =
  | "objection"
  | "mistake"
  | "highlight"
  | "closing_attempt";

export type CallMomentSeverity = "low" | "medium" | "high";

export type CallMoment = {
  timestamp?: number;
  type: CallMomentType;
  text: string;
  severity?: CallMomentSeverity;
};

export type VoiceAnalysis = {
  clarity: number;
  confidence: number;
  filler_density: number;
  pace: number;
  overall: number;
};

export type CallAnalysisStages = {
  intro: StageScore;
  discovery: StageScore;
  objection: StageScore;
  close: StageScore;
};

export type CallAnalysis = {
  overall: number;
  stages: CallAnalysisStages;
  moments: CallMoment[];
  suggestions: string[];
  summary: string;
  voice?: VoiceAnalysis;
};
