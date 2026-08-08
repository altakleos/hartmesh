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
{{- printf "%s-home" (include "deer-flow.fullname" .) -}}
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

{{- define "deer-flow.validate" -}}
{{- include "deer-flow.validateDigest" (dict "name" "gateway" "digest" .Values.gateway.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "frontend" "digest" .Values.frontend.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "provisioner" "digest" .Values.provisioner.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "nginx" "digest" .Values.nginx.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "postgres" "digest" .Values.postgresql.image.digest) -}}
{{- include "deer-flow.validateDigest" (dict "name" "redis" "digest" .Values.redis.image.digest) -}}

{{- $mode := .Values.deployment.mode -}}
{{- if not (has $mode (list "local_evaluation" "durable_one_replica")) -}}
{{- fail "deployment.mode must be local_evaluation or durable_one_replica" -}}
{{- end -}}
{{- $tier := .Values.deployment.persistenceTier -}}
{{- if not (has $tier (list "process_local" "node_durable" "shared_durable")) -}}
{{- fail "deployment.persistenceTier must be process_local, node_durable, or shared_durable" -}}
{{- end -}}
{{- if ne (int .Values.gateway.replicas) 1 -}}
{{- fail "the supported chart topology requires gateway.replicas=1" -}}
{{- end -}}

{{- $appConfig := (.Values.config | fromYaml) -}}
{{- if not (kindIs "map" $appConfig) -}}
{{- fail "config must contain one YAML object" -}}
{{- end -}}
{{- $deploymentConfig := (index $appConfig "deployment") | default dict -}}
{{- $readinessConfig := (index $deploymentConfig "readiness") | default dict -}}
{{- $shutdownConfig := (index $deploymentConfig "shutdown") | default dict -}}
{{- $memoryConfig := (index $appConfig "memory") | default dict -}}
{{- $databaseConfig := (index $appConfig "database") | default dict -}}
{{- $databaseBackend := ((index $databaseConfig "backend") | default "memory") -}}
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

{{- if eq $mode "durable_one_replica" -}}
  {{- if not .Values.gateway.image.digest -}}{{- fail "production validation requires a gateway image digest" -}}{{- end -}}
  {{- if and .Values.provisioner.enabled (not .Values.provisioner.image.digest) -}}{{- fail "production validation requires a provisioner image digest" -}}{{- end -}}
  {{- if ne $tier "shared_durable" -}}{{- fail "durable_one_replica requires shared_durable persistence" -}}{{- end -}}
  {{- if ne ((index $deploymentConfig "profile") | default "local_development") "durable_production" -}}
  {{- fail "durable_one_replica requires config deployment.profile=durable_production" -}}
  {{- end -}}
  {{- if ne $databaseBackend "postgres" -}}
  {{- fail "durable_one_replica requires config database.backend=postgres" -}}
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
