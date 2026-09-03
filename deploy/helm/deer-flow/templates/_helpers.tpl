{{/*
Common helpers for the DeerFlow chart.
*/}}

{{- define "deer-flow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "deer-flow.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "deer-flow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "deer-flow.labels" -}}
helm.sh/chart: {{ include "deer-flow.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "deer-flow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "deer-flow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "deer-flow.namespace" -}}
{{- default .Release.Namespace .Values.namespace -}}
{{- end -}}

{{- define "deer-flow.sandboxNamespace" -}}
{{- default (include "deer-flow.namespace" .) .Values.sandboxNamespace -}}
{{- end -}}

{{/* Cluster-scoped RBAC names include the release namespace so one release per
     tenant remains collision-free even when every release has the same name. */}}
{{- define "deer-flow.provisionerClusterRoleName" -}}
{{- printf "%s-provisioner-ns-%s" (include "deer-flow.fullname" .) (include "deer-flow.namespace" .) -}}
{{- end -}}

{{- define "deer-flow.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}

{{/* Render repository@digest when pinned, otherwise repository:tag. */}}
{{- define "deer-flow.imageReference" -}}
{{- $repository := required "image repository must not be empty" .repository -}}
{{- if or (contains "@" $repository) (contains "://" $repository) (regexMatch "(^|/)[^/]+:[^/]+$" $repository) -}}
{{- fail "image repository must not contain a scheme, tag, credential marker, or digest" -}}
{{- end -}}
{{- if .digest -}}
{{- printf "%s@%s" $repository .digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (required "image tag must not be empty when digest is unset" .tag) -}}
{{- end -}}
{{- end -}}

{{/* Fully-qualified image refs for the three DeerFlow images. Per-workload
     values take precedence; the legacy shared registry/name/tag shape remains
     an upgrade-compatible fallback. */}}
{{- define "deer-flow.gatewayImage" -}}
{{- $repository := .Values.gateway.image.repository -}}
{{- if not $repository -}}
  {{- if .Values.image.registry -}}{{- $repository = printf "%s/%s" .Values.image.registry .Values.image.gatewayImage -}}
  {{- else -}}{{- $repository = .Values.image.gatewayImage -}}{{- end -}}
{{- end -}}
{{- include "deer-flow.imageReference" (dict "repository" $repository "tag" (.Values.gateway.image.tag | default .Values.image.tag) "digest" .Values.gateway.image.digest) -}}
{{- end -}}

{{- define "deer-flow.frontendImage" -}}
{{- $repository := .Values.frontend.image.repository -}}
{{- if not $repository -}}
  {{- if .Values.image.registry -}}{{- $repository = printf "%s/%s" .Values.image.registry .Values.image.frontendImage -}}
  {{- else -}}{{- $repository = .Values.image.frontendImage -}}{{- end -}}
{{- end -}}
{{- include "deer-flow.imageReference" (dict "repository" $repository "tag" (.Values.frontend.image.tag | default .Values.image.tag) "digest" .Values.frontend.image.digest) -}}
{{- end -}}

{{- define "deer-flow.provisionerImage" -}}
{{- $repository := .Values.provisioner.image.repository -}}
{{- if not $repository -}}
  {{- if .Values.image.registry -}}{{- $repository = printf "%s/%s" .Values.image.registry .Values.image.provisionerImage -}}
  {{- else -}}{{- $repository = .Values.image.provisionerImage -}}{{- end -}}
{{- end -}}
{{- include "deer-flow.imageReference" (dict "repository" $repository "tag" (.Values.provisioner.image.tag | default .Values.image.tag) "digest" .Values.provisioner.image.digest) -}}
{{- end -}}

{{- define "deer-flow.nginxImage" -}}
{{- include "deer-flow.imageReference" .Values.nginx.image -}}
{{- end -}}

{{- define "deer-flow.postgresImage" -}}
{{- include "deer-flow.imageReference" .Values.postgresql.image -}}
{{- end -}}

{{- define "deer-flow.redisImage" -}}
{{- include "deer-flow.imageReference" .Values.redis.image -}}
{{- end -}}

{{- define "deer-flow.serviceAccountName" -}}
{{- default (printf "%s-gateway" (include "deer-flow.fullname" .)) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "deer-flow.provisionerServiceAccountName" -}}
{{- default (printf "%s-provisioner" (include "deer-flow.fullname" .)) .Values.provisioner.serviceAccount.name -}}
{{- end -}}

{{/* PVC name for the .deer-flow home directory. */}}
{{- define "deer-flow.homePVC" -}}
{{- default (printf "%s-home" (include "deer-flow.fullname" .)) .Values.persistence.home.existingClaim -}}
{{- end -}}

{{/* Name of the Secret holding provider/channel keys. */}}
{{- define "deer-flow.providerSecret" -}}
{{- if .Values.existingSecret -}}{{- .Values.existingSecret -}}
{{- else -}}{{- printf "%s-provider" (include "deer-flow.fullname" .) -}}{{- end -}}
{{- end -}}

{{/* Name of the Secret holding generated app secrets (auth token, better-auth). */}}
{{- define "deer-flow.appSecret" -}}
{{- printf "%s-app" (include "deer-flow.fullname" .) -}}
{{- end -}}

{{/* Name of the postgres StatefulSet/Service. */}}
{{- define "deer-flow.postgresFullname" -}}
{{- printf "%s-postgres" (include "deer-flow.fullname" .) -}}
{{- end -}}

{{/* Name of the Secret holding DATABASE_URL (and, in bundled mode, the
     postgres superuser password). Resolution order:
       1. postgresql.external.existingSecret (user-managed, key=database-url)
       2. postgresql.existingSecret          (user-managed, bundled image)
       3. chart-managed secret `<release>-postgres`
     Only #3 is created by this chart; #1/#2 must exist already. */}}
{{- define "deer-flow.databaseUrlSecret" -}}
{{- if .Values.postgresql.external.existingSecret -}}{{- .Values.postgresql.external.existingSecret -}}
{{- else if .Values.postgresql.existingSecret -}}{{- .Values.postgresql.existingSecret -}}
{{- else -}}{{- include "deer-flow.postgresFullname" . -}}{{- end -}}
{{- end -}}

{{/* Name of the redis StatefulSet/Service. */}}
{{- define "deer-flow.redisFullname" -}}
{{- printf "%s-redis" (include "deer-flow.fullname" .) -}}
{{- end -}}

{{/* Name of the Secret holding the redis stream-bridge URL (key `redis-url`,
     plus `redis-password` in bundled mode when a password is set). Resolution:
       1. redis.external.existingSecret (user-managed, key=redis-url)
       2. redis.existingSecret          (user-managed, bundled image)
       3. chart-managed secret `<release>-redis`
     Only #3 is created by this chart; #1/#2 must exist already. */}}
{{- define "deer-flow.redisUrlSecret" -}}
{{- if .Values.redis.external.existingSecret -}}{{- .Values.redis.external.existingSecret -}}
{{- else if .Values.redis.existingSecret -}}{{- .Values.redis.existingSecret -}}
{{- else -}}{{- include "deer-flow.redisFullname" . -}}{{- end -}}
{{- end -}}

{{/* Whether any redis stream-bridge backend is configured (bundled StatefulSet,
     external URL, or a user-managed Secret). Drives the env injection in the
     gateway deployment. */}}
{{- define "deer-flow.redisConfigured" -}}
{{- or .Values.redis.enabled .Values.redis.external.redisUrl .Values.redis.external.existingSecret .Values.redis.existingSecret -}}
{{- end -}}

{{/* Canonical TenantIdentityV1 projection. Keep this byte-for-byte aligned
     with runtime/tenant_identity.py canonical JSON and public-ref rules. */}}
{{- define "deer-flow.tenantId" -}}
{{- $tenantId := .Values.tenant.id | default "" -}}
{{- if and $tenantId (not (regexMatch "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$" $tenantId)) -}}
{{- fail "tenant.id must be a lowercase DNS label of 1-63 characters" -}}
{{- end -}}
{{- $tenantId -}}
{{- end -}}

{{- define "deer-flow.tenantPublicRef" -}}
{{- $tenantId := include "deer-flow.tenantId" . | default "local" -}}
{{- $canonical := printf "{\"tenant_id\":\"%s\",\"version\":1}" $tenantId -}}
{{- $digest := sha256sum $canonical -}}
{{- printf "tenant-%s" (substr 0 16 $digest) -}}
{{- end -}}

{{- define "deer-flow.redisTenantPrefix" -}}
{{- printf "hm:v1:%s:redis" (include "deer-flow.tenantPublicRef" .) -}}
{{- end -}}

{{/* Gateway Redis prefixes. A noncanonical selection requires both the
     configured compatibility field and the exact operator-declared copy of
     the database migration record. Runtime startup verifies that DB record. */}}
{{- define "deer-flow.redisKeyPrefixEnv" -}}
{{- $streamBridge := include "deer-flow.redisTenantPrefix" . -}}
{{- $checkpointCache := printf "%s:ckpt-hist:v1" $streamBridge -}}
{{- $sandboxOwnership := printf "%s:deerflow:sandbox:owner" $streamBridge -}}
{{- $recordedStream := .Values.tenant.legacyRedisPrefixes.streamBridge | default "" | trimSuffix ":" -}}
{{- $recordedCheckpoint := .Values.tenant.legacyRedisPrefixes.checkpointCache | default "" | trimSuffix ":" -}}
{{- $recordedOwnership := .Values.tenant.legacyRedisPrefixes.sandboxOwnership | default "" | trimSuffix ":" -}}
{{- $legacyTenant := .Values.redis.tenantPrefix | default "" -}}
{{- $legacyTenant = $legacyTenant | trimSuffix ":" -}}
{{- if and $legacyTenant (and (ne $legacyTenant $streamBridge) (ne $legacyTenant $recordedStream)) -}}
{{- fail "redis.tenantPrefix conflicts with the canonical or operator-recorded legacy tenant namespace" -}}
{{- end -}}
{{- $configuredStream := .Values.redis.keyPrefixes.streamBridge | default "" -}}
{{- $configuredStream = $configuredStream | trimSuffix ":" -}}
{{- if and $configuredStream (and (ne $configuredStream $streamBridge) (ne $configuredStream $recordedStream)) -}}
{{- fail "redis.keyPrefixes.streamBridge conflicts with the canonical or operator-recorded legacy tenant namespace" -}}
{{- end -}}
{{- if and $legacyTenant $configuredStream (ne $legacyTenant $configuredStream) -}}
{{- fail "redis.tenantPrefix conflicts with redis.keyPrefixes.streamBridge" -}}
{{- end -}}
{{- $configuredCheckpoint := .Values.redis.keyPrefixes.checkpointCache | default "" -}}
{{- $configuredCheckpoint = $configuredCheckpoint | trimSuffix ":" -}}
{{- if and $configuredCheckpoint (and (ne $configuredCheckpoint $checkpointCache) (ne $configuredCheckpoint $recordedCheckpoint)) -}}
{{- fail "redis.keyPrefixes.checkpointCache conflicts with the canonical or operator-recorded legacy tenant namespace" -}}
{{- end -}}
{{- $configuredOwnership := .Values.redis.keyPrefixes.sandboxOwnership | default "" -}}
{{- $configuredOwnership = $configuredOwnership | trimSuffix ":" -}}
{{- if and $configuredOwnership (and (ne $configuredOwnership $sandboxOwnership) (ne $configuredOwnership $recordedOwnership)) -}}
{{- fail "redis.keyPrefixes.sandboxOwnership conflicts with the canonical or operator-recorded legacy tenant namespace" -}}
{{- end -}}
- name: DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX
  value: {{ ($configuredStream | default $legacyTenant | default $streamBridge) | quote }}
- name: DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX
  value: {{ ($configuredCheckpoint | default $checkpointCache) | quote }}
- name: DEER_FLOW_SANDBOX_OWNERSHIP_KEY_PREFIX
  value: {{ ($configuredOwnership | default $sandboxOwnership) | quote }}
{{- end -}}

{{/* Exact config bytes mounted by the Gateway and migration hook. Keep the
     profile-derived fields in one renderer so the topology digest cannot
     describe different bytes from the runtime ConfigMap. */}}
{{- define "deer-flow.renderedConfig" -}}
{{- $renderedConfig := ((.Values.config | default "") | fromYaml) | default dict -}}
{{- $sandboxConfig := (index $renderedConfig "sandbox") | default dict -}}
{{- $_ := set $sandboxConfig "accepted_skill_projection_profile" (.Values.provisioner.acceptedSkillProjectionProfile | default "disabled") -}}
{{- if .Values.provisioner.enabled -}}
{{- $_ := set $sandboxConfig "provisioner_service_account_token_file" "/var/run/secrets/hartmesh-provisioner/token" -}}
{{- end -}}
{{- if eq (.Values.provisioner.acceptedSkillProjectionProfile | default "disabled") "rwx_verified_copy_v2" -}}
{{- $runOwnership := (index $renderedConfig "run_ownership") | default dict -}}
{{- $_ := set $runOwnership "heartbeat_enabled" true -}}
{{- $_ := set $renderedConfig "run_ownership" $runOwnership -}}
{{- end -}}
{{- $_ := set $renderedConfig "sandbox" $sandboxConfig -}}
{{- toYaml $renderedConfig -}}
{{- end -}}

{{/* SHA256 checksums of the ConfigMaps. Mount these as pod-template
     annotations: ConfigMaps mounted via subPath do NOT receive live updates,
     so a `helm upgrade` that only changes a ConfigMap would leave pods on stale
     config. A checksum annotation makes any content change alter the pod spec,
     which triggers a rolling restart. */}}
{{- define "deer-flow.configChecksum" -}}
{{- include (print $.Template.BasePath "/configmap-config.yaml") . | sha256sum -}}
{{- end -}}

{{- define "deer-flow.extensionsChecksum" -}}
{{- include (print $.Template.BasePath "/configmap-extensions.yaml") . | sha256sum -}}
{{- end -}}

{{- define "deer-flow.nginxChecksum" -}}
{{- include (print $.Template.BasePath "/configmap-nginx.yaml") . | sha256sum -}}
{{- end -}}

{{/* Percent-encode a string for safe interpolation into a URL userinfo
     (password) segment of a DSN. Sprig lacks urlqueryescape, and
     regexReplaceAllLiteral treats `replacement` as a regex template so chars
     like `[`, `]`, `?` break it - so we chain plain `replace` calls instead.
     `%` is encoded first to avoid double-encoding the percent signs emitted
     for the other characters. Covers the URL-special chars a managed-DB
     password might contain (`@ : / # ? % [ ]` and space). */}}
{{- define "deer-flow.urlEscape" -}}
{{- $s := . -}}
{{- $s = replace "%" "%25" $s -}}
{{- $s = replace "@" "%40" $s -}}
{{- $s = replace ":" "%3A" $s -}}
{{- $s = replace "/" "%2F" $s -}}
{{- $s = replace "#" "%23" $s -}}
{{- $s = replace "?" "%3F" $s -}}
{{- $s = replace "[" "%5B" $s -}}
{{- $s = replace "]" "%5D" $s -}}
{{- $s = replace " " "%20" $s -}}
{{- $s -}}
{{- end -}}

{{/* Fail-closed chart contract validation. Keep validation at render time so
     `helm lint`, `helm template`, install, and upgrade share one decision. */}}
{{- define "deer-flow.validateDigest" -}}
{{- if and .digest (not (regexMatch "^sha256:[0-9a-f]{64}$" .digest)) -}}
{{- fail (printf "%s image digest must be sha256 followed by 64 lowercase hexadecimal characters" .name) -}}
{{- end -}}
{{- end -}}

{{- define "deer-flow.validateSandboxVolumeMode" -}}
{{- $mode := .Values.sandbox.volumeMode | default "" -}}
{{- $homeClaim := .Values.persistence.home.enabled -}}
{{- $skillsClaim := ne (.Values.skills.existingClaim | default "") "" -}}
{{- if and (has $mode (list "" "pvc")) (ne $homeClaim $skillsClaim) -}}
{{- fail "persistence.home.enabled and skills.existingClaim must be configured together when sandbox.volumeMode is empty or pvc; set skills.existingClaim (PVC mode needs both claims), disable persistence.home.enabled with no skills claim, or set sandbox.volumeMode: hostpath for legacy local/hybrid installs" -}}
{{- end -}}
{{- end -}}

{{- define "deer-flow.validate" -}}
{{- include "deer-flow.validateDigest" (dict "name" "gateway" "digest" .Values.gateway.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "frontend" "digest" .Values.frontend.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "provisioner" "digest" .Values.provisioner.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "nginx" "digest" .Values.nginx.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "postgres" "digest" .Values.postgresql.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "redis" "digest" .Values.redis.image.digest) -}}

{{- $mode := .Values.deployment.mode -}}
{{- $tenantId := include "deer-flow.tenantId" . -}}
{{- if not (has $mode (list "local_evaluation" "durable_one_replica" "durable_two_gateway_v1")) -}}
{{- fail "deployment.mode must be local_evaluation, durable_one_replica, or durable_two_gateway_v1" -}}
{{- end -}}
{{- $tier := .Values.deployment.persistenceTier -}}
{{- if not (has $tier (list "process_local" "node_durable" "shared_durable")) -}}
{{- fail "deployment.persistenceTier must be process_local, node_durable, or shared_durable" -}}
{{- end -}}
{{- if eq $mode "durable_two_gateway_v1" -}}
  {{- if ne (int .Values.gateway.replicas) 2 -}}
  {{- fail "durable_two_gateway_v1 requires gateway.replicas=2" -}}
  {{- end -}}
{{- else if ne (int .Values.gateway.replicas) 1 -}}
{{- fail "the supported chart topology requires gateway.replicas=1" -}}
{{- end -}}

{{- $appConfig := (.Values.config | fromYaml) -}}
{{- if not (kindIs "map" $appConfig) -}}
{{- fail "config must contain one YAML object" -}}
{{- end -}}
{{- $deploymentConfig := (index $appConfig "deployment") | default dict -}}
{{- $pluginsConfig := (index $appConfig "plugins") | default list -}}
{{- $hasEnabledExtensions := false -}}
{{- range $pluginsConfig -}}
  {{- if and (kindIs "map" .) (or (not (hasKey . "enabled")) (index . "enabled")) -}}
    {{- $hasEnabledExtensions = true -}}
  {{- end -}}
{{- end -}}
{{- $artifactManifestDigest := .Values.extensions.artifactManifestDigest | default "" -}}
{{- $extensionConfigurationDigest := .Values.extensions.configurationDigest | default "" -}}
{{- if ne (not (empty $artifactManifestDigest)) (not (empty $extensionConfigurationDigest)) -}}
{{- fail "extensions artifactManifestDigest and configurationDigest must be supplied together" -}}
{{- end -}}
{{- if and $artifactManifestDigest (not (regexMatch "^sha256:[0-9a-f]{64}$" $artifactManifestDigest)) -}}
{{- fail "extensions artifactManifestDigest must be a lowercase SHA-256 digest" -}}
{{- end -}}
{{- if and $extensionConfigurationDigest (not (regexMatch "^sha256:[0-9a-f]{64}$" $extensionConfigurationDigest)) -}}
{{- fail "extensions configurationDigest must be a lowercase SHA-256 digest" -}}
{{- end -}}
{{- $readinessConfig := (index $deploymentConfig "readiness") | default dict -}}
{{- $shutdownConfig := (index $deploymentConfig "shutdown") | default dict -}}
{{- $memoryConfig := (index $appConfig "memory") | default dict -}}
{{- $databaseConfig := (index $appConfig "database") | default dict -}}
{{- $databaseBackend := ((index $databaseConfig "backend") | default "memory") -}}
{{- $receiptConfig := (index $appConfig "dedupe_storage") | default dict -}}
{{- $receiptBackend := ((index $receiptConfig "backend") | default "auto") -}}
{{- $runOwnershipConfig := (index $appConfig "run_ownership") | default dict -}}
{{- $runLeaseSeconds := ((index $runOwnershipConfig "lease_seconds") | default 30) -}}
{{- if not (has $receiptBackend (list "auto" "memory" "postgres")) -}}
{{- fail "config dedupe_storage.backend must be auto, memory, or postgres" -}}
{{- end -}}
{{- $tierByBackend := dict "memory" "process_local" "sqlite" "node_durable" "postgres" "shared_durable" -}}
{{- if not (hasKey $tierByBackend $databaseBackend) -}}
{{- fail "config database.backend must be memory, sqlite, or postgres" -}}
{{- end -}}
{{- if ne $tier (index $tierByBackend $databaseBackend) -}}
{{- fail (printf "deployment.persistenceTier must match config database.backend=%s" $databaseBackend) -}}
{{- end -}}
{{- $overallTimeout := 5.0 -}}
{{- if hasKey $readinessConfig "overall_timeout_seconds" -}}
  {{- $overallTimeout = float64 (index $readinessConfig "overall_timeout_seconds") -}}
{{- end -}}
{{- $capabilityTimeout := 2.0 -}}
{{- if hasKey $readinessConfig "capability_probe_timeout_seconds" -}}
  {{- $capabilityTimeout = float64 (index $readinessConfig "capability_probe_timeout_seconds") -}}
{{- end -}}
{{- $probeTimeout := float64 .Values.gateway.readinessProbe.timeoutSeconds -}}
{{- if or (le $capabilityTimeout 0.0) (gt $capabilityTimeout 30.0) -}}
{{- fail "config deployment.readiness capability probe timeout must be greater than 0 and at most 30 seconds" -}}
{{- end -}}
{{- if or (le $overallTimeout 0.0) (gt $overallTimeout 60.0) -}}
{{- fail "config deployment.readiness overall timeout must be greater than 0 and at most 60 seconds" -}}
{{- end -}}
{{- if le $overallTimeout $capabilityTimeout -}}
{{- fail "config deployment.readiness overall timeout must exceed capability probe timeout" -}}
{{- end -}}
{{- if le $probeTimeout $overallTimeout -}}
{{- fail "gateway readiness probe timeout must exceed config deployment.readiness.overall_timeout_seconds" -}}
{{- end -}}
{{- if lt (float64 .Values.gateway.readinessProbe.periodSeconds) $probeTimeout -}}
{{- fail "gateway readiness probe periodSeconds must be at least timeoutSeconds" -}}
{{- end -}}
{{- $livenessTimeout := float64 .Values.gateway.livenessProbe.timeoutSeconds -}}
{{- if le $livenessTimeout 0.0 -}}
{{- fail "gateway liveness probe timeoutSeconds must be positive" -}}
{{- end -}}
{{- if lt (float64 .Values.gateway.livenessProbe.periodSeconds) $livenessTimeout -}}
{{- fail "gateway liveness probe periodSeconds must be at least timeoutSeconds" -}}
{{- end -}}
{{- if or (lt (int .Values.gateway.readinessProbe.failureThreshold) 1) (lt (int .Values.gateway.livenessProbe.failureThreshold) 1) -}}
{{- fail "gateway probe failure thresholds must be positive" -}}
{{- end -}}
{{- if or (lt (int .Values.gateway.readinessProbe.initialDelaySeconds) 0) (lt (int .Values.gateway.livenessProbe.initialDelaySeconds) 0) -}}
{{- fail "gateway probe initial delays must be non-negative" -}}
{{- end -}}
{{- if or (lt (int .Values.gateway.readinessProbe.periodSeconds) 1) (lt (int .Values.gateway.livenessProbe.periodSeconds) 1) -}}
{{- fail "gateway probe periods must be positive" -}}
{{- end -}}
{{- if or (lt (int .Values.gateway.preStopSleepSeconds) 0) (lt (int .Values.gateway.shutdownSchedulingHeadroomSeconds) 1) -}}
{{- fail "gateway preStop must be non-negative and shutdown scheduling headroom must be positive" -}}
{{- end -}}
{{- $admissionSeconds := 2.0 -}}
{{- if hasKey $shutdownConfig "admission_seconds" -}}{{- $admissionSeconds = float64 (index $shutdownConfig "admission_seconds") -}}{{- end -}}
{{- $channelSeconds := 5.0 -}}
{{- if hasKey $shutdownConfig "channel_seconds" -}}{{- $channelSeconds = float64 (index $shutdownConfig "channel_seconds") -}}{{- end -}}
{{- $schedulerSeconds := 3.0 -}}
{{- if hasKey $shutdownConfig "scheduler_seconds" -}}{{- $schedulerSeconds = float64 (index $shutdownConfig "scheduler_seconds") -}}{{- end -}}
{{- $runSeconds := 8.0 -}}
{{- if hasKey $shutdownConfig "run_seconds" -}}{{- $runSeconds = float64 (index $shutdownConfig "run_seconds") -}}{{- end -}}
{{- $memorySeconds := 30.0 -}}
{{- if hasKey $memoryConfig "shutdown_flush_timeout_seconds" -}}{{- $memorySeconds = float64 (index $memoryConfig "shutdown_flush_timeout_seconds") -}}{{- end -}}
{{- $dependencySeconds := 5.0 -}}
{{- if hasKey $shutdownConfig "dependencies_seconds" -}}{{- $dependencySeconds = float64 (index $shutdownConfig "dependencies_seconds") -}}{{- end -}}
{{- range $name, $bounds := dict
      "admission_seconds" (list $admissionSeconds 60.0)
      "channel_seconds" (list $channelSeconds 60.0)
      "scheduler_seconds" (list $schedulerSeconds 60.0)
      "run_seconds" (list $runSeconds 120.0)
      "dependencies_seconds" (list $dependencySeconds 60.0) -}}
  {{- $value := float64 (index $bounds 0) -}}
  {{- $maximum := float64 (index $bounds 1) -}}
  {{- if or (le $value 0.0) (gt $value $maximum) -}}
  {{- fail (printf "application shutdown budget %s must be greater than 0 and at most %.0f seconds" $name $maximum) -}}
  {{- end -}}
{{- end -}}
{{- if or (lt $memorySeconds 1.0) (gt $memorySeconds 300.0) -}}
{{- fail "memory shutdown flush budget must be at least 1 and at most 300 seconds" -}}
{{- end -}}
{{- $shutdownTotal := addf $admissionSeconds $channelSeconds $schedulerSeconds $runSeconds $memorySeconds $dependencySeconds -}}
{{- $requiredTermination := int (ceil (addf $shutdownTotal (.Values.gateway.preStopSleepSeconds | default 0) (.Values.gateway.shutdownSchedulingHeadroomSeconds | default 3))) -}}
{{- if not (kindIs "invalid" .Values.gateway.terminationGracePeriodSeconds) -}}
  {{- if lt (int .Values.gateway.terminationGracePeriodSeconds) $requiredTermination -}}
  {{- fail (printf "gateway terminationGracePeriodSeconds must be at least %d (application shutdown + preStop + headroom)" $requiredTermination) -}}
  {{- end -}}
{{- end -}}

{{- if and (not .Values.serviceAccount.create) (not .Values.serviceAccount.name) -}}
{{- fail "serviceAccount.name is required when serviceAccount.create=false" -}}
{{- end -}}
{{- if and .Values.provisioner.enabled (not .Values.provisioner.serviceAccount.create) (not .Values.provisioner.serviceAccount.name) -}}
{{- fail "provisioner.serviceAccount.name is required when create=false" -}}
{{- end -}}
{{- if and .Values.provisioner.enabled (not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$" (.Values.provisioner.gatewayTokenAudience | default ""))) -}}
{{- fail "provisioner.gatewayTokenAudience must be 1-128 bounded ASCII identifier characters" -}}
{{- end -}}
{{- $acceptedSkillProfile := (default "disabled" .Values.provisioner.acceptedSkillProjectionProfile) -}}
{{- if not (has $acceptedSkillProfile (list "disabled" "rwx_verified_copy_v2")) -}}
{{- fail "provisioner.acceptedSkillProjectionProfile must be disabled or rwx_verified_copy_v2" -}}
{{- end -}}
{{- if eq $acceptedSkillProfile "rwx_verified_copy_v2" -}}
{{- if lt (int .Values.provisioner.acceptedAttempt.leaseSeconds) (mul 2 (int .Values.provisioner.acceptedAttempt.reconcileIntervalSeconds)) -}}
{{- fail "provisioner.acceptedAttempt.leaseSeconds must be at least twice reconcileIntervalSeconds" -}}
{{- end -}}
{{- if lt (int .Values.provisioner.acceptedAttempt.leaseSeconds) (mul 2 (int $runLeaseSeconds)) -}}
{{- fail "provisioner.acceptedAttempt.leaseSeconds must be at least twice config run_ownership.lease_seconds" -}}
{{- end -}}
{{- if or (lt (int .Values.provisioner.acceptedAttempt.reconcileLimit) 1) (gt (int .Values.provisioner.acceptedAttempt.reconcileLimit) 500) -}}
{{- fail "provisioner.acceptedAttempt.reconcileLimit must be in [1, 500]" -}}
{{- end -}}
  {{- if not .Values.provisioner.enabled -}}
  {{- fail "rwx_verified_copy_v2 requires the provisioner" -}}
  {{- end -}}
  {{- if not .Values.persistence.home.enabled -}}
  {{- fail "rwx_verified_copy_v2 requires the shared home PVC" -}}
  {{- end -}}
  {{- if ne .Values.persistence.home.accessMode "ReadWriteMany" -}}
  {{- fail "rwx_verified_copy_v2 requires persistence.home.accessMode=ReadWriteMany; same-node RWO fallback is unsupported" -}}
  {{- end -}}
  {{- if not .Values.provisioner.image.digest -}}
  {{- fail "rwx_verified_copy_v2 requires a digest-pinned provisioner verifier/gate image" -}}
  {{- end -}}
  {{- if not (regexMatch "^[^[:space:]@]+@sha256:[0-9a-f]{64}$" .Values.provisioner.sandboxImage) -}}
  {{- fail "rwx_verified_copy_v2 requires provisioner.sandboxImage pinned by sha256 digest" -}}
  {{- end -}}
{{- end -}}

{{- if gt (len .Values.gateway.extraEnvFrom) 16 -}}
{{- fail "gateway extraEnvFrom is limited to 16 references" -}}
{{- end -}}
{{- range .Values.gateway.extraEnvFrom -}}
  {{- $hasSecret := hasKey . "secretRef" -}}
  {{- $hasConfigMap := hasKey . "configMapRef" -}}
  {{- if eq $hasSecret $hasConfigMap -}}
  {{- fail "gateway extraEnvFrom entries must contain exactly one secretRef or configMapRef" -}}
  {{- end -}}
  {{- range $key, $_ := . -}}
    {{- if not (has $key (list "secretRef" "configMapRef" "prefix")) -}}
    {{- fail (printf "gateway extraEnvFrom field %s is unsupported" $key) -}}
    {{- end -}}
  {{- end -}}
  {{- $reference := ternary .secretRef .configMapRef $hasSecret -}}
  {{- if not $reference.name -}}
  {{- fail "gateway extraEnvFrom references require a name" -}}
  {{- end -}}
  {{- range $key, $_ := $reference -}}
    {{- if not (has $key (list "name" "optional")) -}}
    {{- fail (printf "gateway extraEnvFrom reference field %s is unsupported" $key) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{- $reservedVolumes := list "config" "extensions" "skills" "home" -}}
{{- $extraVolumeNames := dict -}}
{{- if gt (len .Values.gateway.extraVolumes) 16 -}}
{{- fail "gateway extraVolumes is limited to 16 references" -}}
{{- end -}}
{{- range .Values.gateway.extraVolumes -}}
  {{- if or (not .name) (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" .name)) (gt (len .name) 63) -}}
  {{- fail "gateway extra volume names must be valid bounded DNS labels" -}}
  {{- end -}}
  {{- if or (has .name $reservedVolumes) (hasKey $extraVolumeNames .name) -}}
  {{- fail (printf "gateway extra volume name %s is reserved or duplicated" .name) -}}
  {{- end -}}
  {{- $_ := set $extraVolumeNames .name true -}}
  {{- $hasSecret := hasKey . "secret" -}}
  {{- $hasConfigMap := hasKey . "configMap" -}}
  {{- if eq $hasSecret $hasConfigMap -}}
  {{- fail "gateway extra volumes must contain exactly one secret or configMap source" -}}
  {{- end -}}
  {{- if ne (len .) 2 -}}
  {{- fail "gateway extra volumes accept only name and one secret or configMap source" -}}
  {{- end -}}
  {{- if and $hasSecret (not .secret.secretName) -}}
  {{- fail "gateway extra secret volumes require secretName" -}}
  {{- end -}}
  {{- if and $hasConfigMap (not .configMap.name) -}}
  {{- fail "gateway extra ConfigMap volumes require name" -}}
  {{- end -}}
  {{- $source := ternary .secret .configMap $hasSecret -}}
  {{- range $key, $_ := $source -}}
    {{- if not (has $key (list "secretName" "name" "items" "defaultMode" "optional")) -}}
    {{- fail (printf "gateway extra volume source field %s is unsupported" $key) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- $extraMountPaths := dict -}}
{{- if gt (len .Values.gateway.extraVolumeMounts) 16 -}}
{{- fail "gateway extraVolumeMounts is limited to 16 references" -}}
{{- end -}}
{{- range .Values.gateway.extraVolumeMounts -}}
  {{- if or (not .name) (not (hasKey $extraVolumeNames .name)) -}}
  {{- fail "gateway extra volume mounts must reference a declared extra volume" -}}
  {{- end -}}
  {{- if not .readOnly -}}
  {{- fail "gateway extra Secret/ConfigMap volume mounts must be readOnly" -}}
  {{- end -}}
  {{- if or (not .mountPath) (not (hasPrefix "/" .mountPath)) (gt (len .mountPath) 256) -}}
  {{- fail "gateway extra volume mountPath must be a bounded absolute path" -}}
  {{- end -}}
  {{- if or (eq .mountPath "/app") (hasPrefix "/app/" .mountPath) (hasKey $extraMountPaths .mountPath) -}}
  {{- fail (printf "gateway extra volume mountPath %s is reserved or duplicated" .mountPath) -}}
  {{- end -}}
  {{- $_ := set $extraMountPaths .mountPath true -}}
  {{- range $key, $_ := . -}}
    {{- if not (has $key (list "name" "mountPath" "readOnly" "subPath")) -}}
    {{- fail (printf "gateway extra volume mount field %s is unsupported" $key) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{- if gt (len .Values.gateway.podLabels) 32 -}}
{{- fail "gateway pod labels are limited to 32 entries" -}}
{{- end -}}
{{- range $key, $value := .Values.gateway.podLabels -}}
  {{- if or (gt (len $key) 253) (has $key (list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component")) (gt (len (toString $value)) 63) -}}
  {{- fail (printf "gateway pod label %s is reserved or exceeds 63 bytes" $key) -}}
  {{- end -}}
{{- end -}}
{{- if gt (len .Values.gateway.podAnnotations) 32 -}}
{{- fail "gateway pod annotations are limited to 32 entries" -}}
{{- end -}}
{{- range $key, $value := .Values.gateway.podAnnotations -}}
  {{- if or (gt (len $key) 253) (hasPrefix "checksum/" $key) (gt (len (toString $value)) 2048) -}}
  {{- fail (printf "gateway pod annotation %s is reserved or exceeds 2048 bytes" $key) -}}
  {{- end -}}
{{- end -}}

{{- $sourceRevision := .Values.deployment.provenance.sourceRevision -}}
{{- $subagentBatchesConfig := (index $appConfig "subagent_batches") | default dict -}}
{{- if and $sourceRevision (not (regexMatch "^[0-9a-f]{7,64}$" $sourceRevision)) -}}
{{- fail "deployment provenance sourceRevision must be 7-64 lowercase hexadecimal characters" -}}
{{- end -}}
{{- if gt (len .Values.deployment.qualificationEvidence) 16 -}}
{{- fail "deployment qualificationEvidence is limited to 16 entries" -}}
{{- end -}}
{{- if gt (len (toJson .Values.deployment.qualificationEvidence)) 16384 -}}
{{- fail "deployment qualificationEvidence is limited to 16 KiB of canonical JSON" -}}
{{- end -}}
{{- range .Values.deployment.qualificationEvidence -}}
  {{- $hasScope := hasKey . "scope" -}}
  {{- $hasStatus := hasKey . "status" -}}
  {{- if ne $hasScope $hasStatus -}}
  {{- fail "deployment qualification scope and status must be supplied together" -}}
  {{- end -}}
  {{- if and (not $hasScope) (ne (len .) 3) -}}
  {{- fail "legacy deployment qualification evidence accepts exactly qualificationId, artifactDigest, and completedAt" -}}
  {{- end -}}
  {{- if and $hasScope (ne (len .) 5) -}}
  {{- fail "scoped deployment qualification evidence accepts exactly qualificationId, artifactDigest, completedAt, scope, and status" -}}
  {{- end -}}
  {{- if not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$" .qualificationId) -}}
  {{- fail "deployment qualificationId must be a bounded safe identifier" -}}
  {{- end -}}
  {{- if not (regexMatch "^sha256:[0-9a-f]{64}$" .artifactDigest) -}}
  {{- fail "deployment qualification artifactDigest must be a lowercase SHA-256 digest" -}}
  {{- end -}}
  {{- if or (gt (len .completedAt) 64) (not (regexMatch "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$" .completedAt)) -}}
  {{- fail "deployment qualification completedAt must be an RFC3339 timestamp" -}}
  {{- end -}}
  {{- if $hasScope -}}
    {{- if not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$" .scope) -}}
    {{- fail "deployment qualification scope must be a bounded safe identifier" -}}
    {{- end -}}
    {{- if ne .status "passed" -}}
    {{- fail "deployment qualification status must be passed" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{- $candidate := .Values.deployment.qualificationCandidate | default dict -}}
{{- $candidateEnabled := (index $candidate "enabled") | default false -}}
{{- $candidateId := (index $candidate "id") | default "" -}}
{{- if and $candidateEnabled (not (hasPrefix "hartmesh-qualification-" (include "deer-flow.namespace" .))) -}}
{{- fail "qualification candidate requires a disposable namespace beginning hartmesh-qualification-" -}}
{{- end -}}
{{- if and $candidateEnabled (not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$" $candidateId)) -}}
{{- fail "qualification candidate requires a bounded safe id" -}}
{{- end -}}
{{- if and $candidateEnabled (gt (len .Values.deployment.qualificationEvidence) 0) -}}
{{- fail "qualification candidate cannot declare passing evidence" -}}
{{- end -}}

{{- if eq $mode "durable_two_gateway_v1" -}}
  {{- $profile := ((index $deploymentConfig "profile") | default "local_development") -}}
  {{- $checkpointCache := (index $databaseConfig "checkpoint_cache") | default dict -}}
  {{- $checkpointerConfig := (index $appConfig "checkpointer") | default dict -}}
  {{- $runEventsConfig := (index $appConfig "run_events") | default dict -}}
  {{- $agentStorageConfig := (index $appConfig "agent_storage") | default dict -}}
  {{- $streamBridgeConfig := (index $appConfig "stream_bridge") | default dict -}}
  {{- $schedulerConfig := (index $appConfig "scheduler") | default dict -}}
  {{- $mcpTasksConfig := (index $appConfig "mcp_tasks") | default dict -}}
  {{- $sandboxConfig := (index $appConfig "sandbox") | default dict -}}
  {{- $sandboxOwnership := (index $sandboxConfig "ownership") | default dict -}}
  {{- $channelConnections := (index $appConfig "channel_connections") | default dict -}}
  {{- $legacyChannels := (index $appConfig "channels") | default dict -}}
  {{- $extensionsRuntimeConfig := (.Values.extensionsConfig | default "") | fromJson -}}
  {{- $databaseSchema := (index $databaseConfig "postgres_schema") | default "" -}}
  {{- $checkpointerSchema := (index $checkpointerConfig "postgres_schema") | default "" -}}
  {{- $databaseSchemaRef := .Values.deployment.topology.databaseSchemaRef | default "" -}}
  {{- $globalAutoscaling := (index .Values "autoscaling") | default dict -}}

  {{- if or (not $tenantId) (eq $tenantId "local") -}}
  {{- fail "durable_two_gateway_v1 requires tenant.id to be an explicit non-local identity" -}}
  {{- end -}}
  {{- if ne $tier "shared_durable" -}}
  {{- fail "durable_two_gateway_v1 requires shared_durable persistence" -}}
  {{- end -}}
  {{- if ne $profile "durable_two_gateway_v1" -}}
  {{- fail "durable_two_gateway_v1 requires config deployment.profile=durable_two_gateway_v1" -}}
  {{- end -}}
  {{- if or .Values.gateway.autoscaling.enabled ((index $globalAutoscaling "enabled") | default false) -}}
  {{- fail "durable_two_gateway_v1 forbids Gateway autoscaling" -}}
  {{- end -}}
  {{- if not (regexMatch "^schema:sha256:[0-9a-f]{64}$" $databaseSchemaRef) -}}
  {{- fail "durable_two_gateway_v1 requires deployment.topology.databaseSchemaRef as schema:sha256:<64 lowercase hex>" -}}
  {{- end -}}
  {{- if not $sourceRevision -}}
  {{- fail "durable_two_gateway_v1 requires deployment.provenance.sourceRevision" -}}
  {{- end -}}

  {{- $matchingEvidence := 0 -}}
  {{- range .Values.deployment.qualificationEvidence -}}
    {{- if and (eq (.scope | default "") "durable_two_gateway_v1_postgres_redis_aio_rwx") (eq (.status | default "") "passed") -}}
      {{- $matchingEvidence = add1 $matchingEvidence -}}
    {{- end -}}
  {{- end -}}
  {{- if and (not $candidateEnabled) (ne $matchingEvidence 1) -}}
  {{- fail "durable_two_gateway_v1 requires passed qualification evidence for durable_two_gateway_v1_postgres_redis_aio_rwx" -}}
  {{- end -}}

  {{- if or (not .Values.gateway.image.digest) (not .Values.frontend.image.digest) (not .Values.nginx.image.digest) (not .Values.provisioner.image.digest) (not .Values.postgresql.image.digest) (not .Values.redis.image.digest) -}}
  {{- fail "durable_two_gateway_v1 requires digest-pinned gateway, frontend, nginx, provisioner, PostgreSQL, and Redis images" -}}
  {{- end -}}
  {{- if not (regexMatch "^[^[:space:]@]+@sha256:[0-9a-f]{64}$" .Values.provisioner.sandboxImage) -}}
  {{- fail "durable_two_gateway_v1 requires a digest-pinned provisioner.sandboxImage" -}}
  {{- end -}}
  {{- if ne ((index $sandboxConfig "image") | default "") .Values.provisioner.sandboxImage -}}
  {{- fail "durable_two_gateway_v1 requires config sandbox.image to equal provisioner.sandboxImage" -}}
  {{- end -}}
  {{- if or (not $artifactManifestDigest) (not $extensionConfigurationDigest) -}}
  {{- fail "durable_two_gateway_v1 requires extension artifact and configuration digests" -}}
  {{- end -}}

  {{- if or (ne $databaseBackend "postgres") (not ((index $databaseConfig "command_timeout") | default false)) -}}
  {{- fail "durable_two_gateway_v1 requires PostgreSQL with a finite database.command_timeout" -}}
  {{- end -}}
  {{- if ne ((index $checkpointCache "type") | default "memory") "redis" -}}
  {{- fail "durable_two_gateway_v1 requires database.checkpoint_cache.type=redis" -}}
  {{- end -}}
  {{- if or (ne ((index $checkpointerConfig "type") | default "") "postgres") (ne $checkpointerSchema $databaseSchema) -}}
  {{- fail "durable_two_gateway_v1 requires a PostgreSQL checkpointer on the application schema" -}}
  {{- end -}}
  {{- if ne ((index $runEventsConfig "backend") | default "memory") "db" -}}
  {{- fail "durable_two_gateway_v1 requires run_events.backend=db" -}}
  {{- end -}}
  {{- if ne ((index $agentStorageConfig "backend") | default "file") "db" -}}
  {{- fail "durable_two_gateway_v1 requires agent_storage.backend=db" -}}
  {{- end -}}
  {{- if ne ((index $streamBridgeConfig "type") | default "memory") "redis" -}}
  {{- fail "durable_two_gateway_v1 requires stream_bridge.type=redis" -}}
  {{- end -}}
  {{- if or (not ((index $schedulerConfig "enabled") | default false)) (not ((index $schedulerConfig "multi_instance") | default false)) -}}
  {{- fail "durable_two_gateway_v1 requires scheduler.enabled and scheduler.multi_instance" -}}
  {{- end -}}
  {{- if ((index $subagentBatchesConfig "enabled") | default false) -}}
  {{- fail "subagent_batches_exact_two_unqualified: durable subagent batches are not qualified for durable_two_gateway_v1" -}}
  {{- end -}}
  {{- if not ((index $mcpTasksConfig "enabled") | default false) -}}
  {{- fail "durable_two_gateway_v1 requires mcp_tasks.enabled" -}}
  {{- end -}}
  {{- if not (kindIs "map" $extensionsRuntimeConfig) -}}
  {{- fail "durable_two_gateway_v1 requires valid extensionsConfig JSON" -}}
  {{- end -}}
  {{- $mcpServers := (index $extensionsRuntimeConfig "mcpServers") | default dict -}}
  {{- $taskToolsetCount := 0 -}}
  {{- range $serverName, $server := $mcpServers -}}
    {{- if and (kindIs "map" $server) (or (not (hasKey $server "enabled")) (index $server "enabled")) -}}
      {{- $taskToolsetCount = add $taskToolsetCount (len ((index $server "task_toolsets") | default list)) -}}
    {{- end -}}
  {{- end -}}
  {{- if lt $taskToolsetCount 1 -}}
  {{- fail "durable_two_gateway_v1 requires at least one enabled durable MCP task toolset" -}}
  {{- end -}}
  {{- if not ((index $runOwnershipConfig "heartbeat_enabled") | default false) -}}
  {{- fail "durable_two_gateway_v1 requires run_ownership.heartbeat_enabled" -}}
  {{- end -}}
  {{- if not (has $receiptBackend (list "auto" "postgres")) -}}
  {{- fail "durable_two_gateway_v1 requires shared PostgreSQL inbound dedupe" -}}
  {{- end -}}

  {{- if or .Values.postgresql.enabled (not .Values.postgresql.external.existingSecret) .Values.postgresql.external.databaseUrl .Values.postgresql.auth.password .Values.postgresql.existingSecret -}}
  {{- fail "durable_two_gateway_v1 requires a pre-existing external PostgreSQL Secret for migration authority" -}}
  {{- end -}}
  {{- if or .Values.redis.enabled (not .Values.redis.external.existingSecret) .Values.redis.external.redisUrl .Values.redis.auth.password .Values.redis.existingSecret -}}
  {{- fail "durable_two_gateway_v1 requires a pre-existing external Redis Secret" -}}
  {{- end -}}
  {{- if .Values.secrets -}}
  {{- fail "durable_two_gateway_v1 forbids inline provider secrets" -}}
  {{- end -}}

  {{- if or (not .Values.provisioner.enabled) (ne (.Values.sandbox.volumeMode | default "") "pvc") (ne (.Values.provisioner.sandboxServiceType | default "ClusterIP") "ClusterIP") -}}
  {{- fail "durable_two_gateway_v1 requires the in-cluster AIO provisioner with ClusterIP PVC mode" -}}
  {{- end -}}
  {{- if not (has ((index $sandboxConfig "use") | default "") (list "deerflow.community.aio_sandbox:AioSandboxProvider" "deerflow.community.aio_sandbox.provider:AioSandboxProvider")) -}}
  {{- fail "durable_two_gateway_v1 requires the AIO sandbox provider" -}}
  {{- end -}}
  {{- if or (ne ((index $sandboxOwnership "type") | default "memory") "redis") (index $sandboxConfig "provisioner_api_key") -}}
  {{- fail "durable_two_gateway_v1 requires Redis sandbox ownership and projected ServiceAccount authentication" -}}
  {{- end -}}
  {{- if ne ((index $sandboxConfig "accepted_materialization_profile") | default "disabled") "disabled" -}}
  {{- fail "durable_two_gateway_v1 forbids unsupported sandbox materialization providers" -}}
  {{- end -}}
  {{- if ne (.Values.provisioner.acceptedSkillProjectionProfile | default "disabled") "rwx_verified_copy_v2" -}}
  {{- fail "durable_two_gateway_v1 requires rwx_verified_copy_v2" -}}
  {{- end -}}
  {{- if or (not .Values.persistence.home.enabled) (not .Values.persistence.home.existingClaim) (not .Values.skills.existingClaim) (ne .Values.persistence.home.accessMode "ReadWriteMany") (ne .Values.skills.accessMode "ReadWriteMany") -}}
  {{- fail "durable_two_gateway_v1 requires ReadWriteMany existing home and skills PVCs" -}}
  {{- end -}}

  {{- $channelsEnabled := ((index $channelConnections "enabled") | default false) -}}
  {{- range $provider := list "slack" "telegram" "discord" "feishu" "dingtalk" "wechat" "wecom" "buzz" "github" -}}
    {{- $providerConfig := (index $channelConnections $provider) | default dict -}}
    {{- if ((index $providerConfig "enabled") | default false) -}}
      {{- $channelsEnabled = true -}}
    {{- end -}}
    {{- $legacyProviderConfig := (index $legacyChannels $provider) | default dict -}}
    {{- if ((index $legacyProviderConfig "enabled") | default false) -}}
      {{- $channelsEnabled = true -}}
    {{- end -}}
  {{- end -}}
  {{- if $channelsEnabled -}}
  {{- fail "durable_two_gateway_v1 forbids IM/channel connectors" -}}
  {{- end -}}

  {{- $enabledPluginCount := 0 -}}
  {{- $pluginsQualified := true -}}
  {{- range $pluginsConfig -}}
    {{- if and (kindIs "map" .) (or (not (hasKey . "enabled")) (index . "enabled")) -}}
      {{- $enabledPluginCount = add1 $enabledPluginCount -}}
      {{- if or (ne ((index . "name") | default "") "governance") (ne ((index . "package") | default "") "hartmesh-governance-extension") (ne ((index . "use") | default "") "hartmesh_governance_extension:install") -}}
        {{- $pluginsQualified = false -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- if or (gt $enabledPluginCount 1) (not $pluginsQualified) -}}
  {{- fail "durable_two_gateway_v1 allows only the qualified governance extension" -}}
  {{- end -}}
  {{- if not $candidateEnabled -}}
  {{- fail "topology_qualification_missing: durable_two_gateway_v1 remains unavailable until a verified qualification artifact is bundled by a future release" -}}
  {{- end -}}
{{- end -}}

{{- if eq $mode "durable_one_replica" -}}
  {{- if ((index $subagentBatchesConfig "enabled") | default false) -}}
  {{- fail "subagent_batches_durable_one_unqualified: durable subagent batches require a passing artifact-bound PostgreSQL process-restart suite" -}}
  {{- end -}}
  {{- if or (not $tenantId) (eq $tenantId "local") -}}{{- fail "durable_one_replica requires tenant.id to be an explicit non-local identity" -}}{{- end -}}
  {{- if not .Values.gateway.image.digest -}}{{- fail "production validation requires a gateway image digest" -}}{{- end -}}
  {{- if and $hasEnabledExtensions (not $artifactManifestDigest) -}}{{- fail "durable_one_replica with enabled extensions requires expected artifact and configuration digests" -}}{{- end -}}
  {{- if and .Values.provisioner.enabled (not .Values.provisioner.image.digest) -}}{{- fail "production validation requires a provisioner image digest" -}}{{- end -}}
  {{- if ne $tier "shared_durable" -}}{{- fail "durable_one_replica requires shared_durable persistence" -}}{{- end -}}
  {{- if ne ((index $deploymentConfig "profile") | default "local_development") "durable_production" -}}
  {{- fail "durable_one_replica requires config deployment.profile=durable_production" -}}
  {{- end -}}
  {{- if ne $databaseBackend "postgres" -}}
  {{- fail "durable_one_replica requires config database.backend=postgres" -}}
  {{- end -}}
  {{- if not ((index $databaseConfig "command_timeout") | default false) -}}
  {{- fail "durable_one_replica requires a finite database.command_timeout" -}}
  {{- end -}}
  {{- if eq $receiptBackend "memory" -}}
  {{- fail "durable_one_replica requires PostgreSQL inbound receipt storage; dedupe_storage.backend cannot be memory" -}}
  {{- end -}}
  {{- $postgresConfigured := or .Values.postgresql.enabled .Values.postgresql.external.databaseUrl .Values.postgresql.external.existingSecret .Values.postgresql.existingSecret -}}
  {{- if not $postgresConfigured -}}{{- fail "durable_one_replica requires a configured PostgreSQL database" -}}{{- end -}}
  {{- if .Values.secrets -}}{{- fail "durable_one_replica forbids inline provider secrets; use existingSecret or gateway.extraEnvFrom" -}}{{- end -}}
  {{- if or .Values.postgresql.auth.password .Values.postgresql.external.databaseUrl -}}
  {{- fail "durable_one_replica forbids inline PostgreSQL credentials; use an existingSecret" -}}
  {{- end -}}
  {{- if not (or .Values.postgresql.external.existingSecret .Values.postgresql.existingSecret) -}}
  {{- fail "durable_one_replica requires PostgreSQL credentials through an existingSecret" -}}
  {{- end -}}
  {{- if or .Values.redis.auth.password .Values.redis.external.redisUrl -}}
  {{- fail "durable_one_replica forbids inline Redis credentials; use an existingSecret" -}}
  {{- end -}}
  {{- $redisConfigured := or .Values.redis.enabled .Values.redis.external.redisUrl .Values.redis.external.existingSecret .Values.redis.existingSecret -}}
  {{- if and $redisConfigured (not (or .Values.redis.external.existingSecret .Values.redis.existingSecret)) -}}
  {{- fail "durable_one_replica requires Redis connection credentials through an existingSecret when Redis is configured" -}}
  {{- end -}}
{{- end -}}
{{- end -}}
