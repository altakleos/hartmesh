import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useAuth } from "@/core/auth/AuthProvider";

import {
  enableSkill,
  SkillRequestError,
  uploadSkillArchive,
  cloneSkill,
  loadSkillCloneSources,
} from "./api";

import { loadSkills } from ".";

export function useSkills() {
  const { user } = useAuth();
  const { data, isLoading, error } = useQuery({
    queryKey: ["skills", user?.id ?? null, user?.system_role ?? null],
    queryFn: () => loadSkills(),
    retry: (count, err) => !(err instanceof SkillRequestError) && count < 3,
  });
  return { skills: data ?? [], isLoading, error };
}

export function useEnableSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({
      skillName,
      enabled,
    }: {
      skillName: string;
      enabled: boolean;
    }) => {
      await enableSkill(skillName, enabled);
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["skills"] });
    },
  });
}

export function useUploadSkillArchive() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (
      input: File | { archive: File; allowBaselineOverride: boolean },
    ) =>
      input instanceof File
        ? uploadSkillArchive(input)
        : uploadSkillArchive(input.archive, input.allowBaselineOverride),
    onSuccess: (result) => {
      if (result.success) {
        void queryClient.invalidateQueries({ queryKey: ["skills"] });
      }
    },
  });
}

export function useSkillCloneSources(enabled: boolean) {
  const { user } = useAuth();
  return useQuery({
    queryKey: [
      "skill-clone-sources",
      user?.id ?? null,
      user?.system_role ?? null,
    ],
    queryFn: ({ signal }) => loadSkillCloneSources(signal),
    enabled,
    retry: false,
    gcTime: 0,
  });
}

export function useCloneSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: cloneSkill,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["skills"] });
    },
  });
}
