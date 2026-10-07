export interface Skill {
  name: string;
  description: string;
  category: string;
  license: string;
  enabled: boolean;
  editable: boolean;
  origin?: {
    source_id: string;
    source_name: string;
    source_category: string;
    revision: string;
  } | null;
  overrides_baseline?: boolean;
}

export interface SkillCloneSource {
  source_id: string;
  name: string;
  category: string;
  enabled: boolean;
}
