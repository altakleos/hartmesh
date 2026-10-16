import type { LucideIcon } from "lucide-react";

export interface StorageUnavailableTranslations {
  serverNotReady: string;
  serverCheckFailed: string;
  agentsPaused: (provider: string) => string;
  serverMoved: (name: string, provider: string) => string;
  filesUnchecked: (name: string) => string;
  agentsWait: string;
  diskTooSmall: (provider: string) => string;
  storageMissing: (provider: string) => string;
  storageUnreadable: (provider: string) => string;
  serverNotReadyFiles: (provider: string) => string;
  serverCheckFailedFiles: (provider: string) => string;
  next: (provider: string) => string;
  reference: (code: string) => string;
  askAdmin: string;
}

export interface Translations {
  people: {
    you: string;
    youInline: string;
    aManager: string;
    formerNamed: (person: string) => string;
    former: string;
    unknown: string;
  };
  chat: {
    filesFull: (support: string) => string;
    filesPreparing: (support: string) => string;
    filesNoSeat: (administrator: boolean, provider: string) => string;
    filesUnavailable: string;
    myFilesUnchecked: string;
  };
  auth: { noSeats: string };
  attention: {
    navCount: (pending: number, routing: number) => string;
    notice: (name?: string) => string;
    noAssessment: (name: string) => string;
    unconfirmed: string;
    noPermission: string;
    unavailableStill: (name?: string) => string;
    loadError: string;
    emptyPending: string;
    recipient: string;
    recipientDefault: (supervisor: string) => string;
    recipientNone: string;
    recipientMissing: string;
    shared: (name: string) => string;
    fileNotice: (name: string) => string;
    savedAt: (time: string) => string;
    readCurrent: string;
    readStale: string;
    sourceUnavailable: string;
    closedReason: {
      resolved: string;
      superseded: string;
      withdrawn: string;
    };
    event: {
      create: string;
      respond: string;
      route: string;
      withdraw: string;
      assess: string;
      decide: string;
      accept: string;
      edit: string;
      cancel: string;
      reopen: string;
      changes_requested: string;
      reconcile_mandate: string;
      other: string;
    };
  } & Record<
    | "accept"
    | "ack"
    | "activity"
    | "all"
    | "answered"
    | "back"
    | "cannot_provide"
    | "choice"
    | "choices"
    | "closed"
    | "create"
    | "createUnavailable"
    | "decide"
    | "decision"
    | "defaultRecipient"
    | "disposition"
    | "empty"
    | "expected"
    | "file"
    | "history"
    | "information"
    | "next"
    | "note"
    | "openWork"
    | "outcome"
    | "path"
    | "pending"
    | "purpose"
    | "question"
    | "read"
    | "readNotice"
    | "readState"
    | "reason"
    | "receipt"
    | "refresh"
    | "requestLink"
    | "reset"
    | "responses"
    | "retry"
    | "review"
    | "route"
    | "routing"
    | "saved"
    | "send"
    | "sources"
    | "space"
    | "stale"
    | "state"
    | "supplied"
    | "text"
    | "title"
    | "unassign"
    | "unavailable"
    | "unchecked"
    | "withdraw"
    | "wrong_recipient",
    string
  >;
  agentWork: {
    execution: (name: string) => string;
    openConversation: string;
    newConversation: string;
    recordsOnly: (name: string) => string;
    reportedComplete: (name: string) => string;
    policyNotice: string;
    due: string;
    inputNotice: (name: string) => string;
    stop: string;
    stopConfirm: (name: string) => string;
    stopNow: string;
    keepWorking: string;
    stopPending: (name: string) => string;
    stopped: (person: string, time: string) => string;
    stoppedByEdit: (person: string) => string;
    stoppedByWithdraw: (person: string) => string;
    stoppedByService: (name: string) => string;
    stoppedPlain: string;
    stopNothing: (name: string) => string;
    stopFailed: string;
    uncertain: (name: string) => string;
    uncertainWaiting: (name: string) => string;
    attemptFailed: (name: string, support: string) => string;
    endedWithoutReport: (name: string) => string;
    recoverActivation: string;
    recoverActivationNote: string;
    cancel: string;
    visibility: (supervisor: string) => string;
    giveNeedsChat: (name: string, supervisor: string) => string;
    reconcileNeeded: (name: string) => string;
    reconcile_mandate: string;
    sharedNotice: (name: string) => string;
    policyDisabled: (name: string) => string;
    advanced: string;
    reconcileAttempt: string;
    containmentReceipt: (name: string) => string;
    containmentNotice: (name: string) => string;
    latest: string;
    manualFirst: (name: string) => string;
    unresolvedAsk: (supervisor: string) => string;
    host: string;
    attemptStatus: {
      starting: string;
      running: string;
      stopping: string;
      uncertain: string;
      succeeded: string;
      failed: string;
      cancelled: string;
      contained: string;
      notStarted: string;
      noResult: string;
    };
    attempt: {
      busy: (name: string, action: string) => string;
      runLimit: (name: string, action: string) => string;
      restarting: (name: string, action: string) => string;
      startingElsewhere: (name: string, action: string) => string;
      startFailed: (name: string, action: string, support: string) => string;
      full: (name: string, support: string) => string;
      fullAsk: (name: string, supervisor: string, support: string) => string;
    };
    workLimit: {
      one: string;
      many: (count: number) => string;
      which: (
        agent: string,
        objective: string,
        time: string,
        person: string,
      ) => string;
      starting: (agent: string) => string;
      free: (name: string) => string;
    };
    event: {
      delegate: string;
      activate: string;
      progress: string;
      outcome: string;
      request_input: string;
      revise_input: string;
      assess_input: string;
      suggest: string;
      derive: string;
      derived: string;
      human_response: string;
      request_route: string;
      request_withdraw: string;
      input: string;
      decide: string;
      accept: string;
      edit: string;
      cancel: string;
      reopen: string;
      changes_requested: string;
      reconcile_mandate: string;
      reconcile_attempt: string;
      stop: string;
      attempt_settled: string;
      attempt_uncertain: string;
      attempt_contained: string;
      other: string;
    };
    contained: {
      containerAbsent: (name: string) => string;
      runOrphaned: (name: string) => string;
      runNeverStarted: (name: string) => string;
      startFailed: (name: string, support: string) => string;
      outcomeUncertain: (name: string) => string;
      idle: (name: string) => string;
      lifecycle: (name: string) => string;
      storageRecovery: (provider: string, name: string) => string;
      operator: (name: string, provider: string) => string;
      hostReplaced: (name: string) => string;
      unknown: (name: string) => string;
      kept: (name: string) => string;
      candidate: (name: string) => string;
    };
  } & Record<
    | "accept"
    | "acceptUnchecked"
    | "acceptedBy"
    | "activate"
    | "addSource"
    | "blocked"
    | "cancelEdit"
    | "cancelled"
    | "candidate"
    | "changes_requested"
    | "completed"
    | "creator"
    | "criteria"
    | "currentWork"
    | "decide"
    | "delegate"
    | "delegateSuggestion"
    | "edit"
    | "empty"
    | "error"
    | "high"
    | "history"
    | "human"
    | "input"
    | "lastActivity"
    | "low"
    | "needsInput"
    | "next"
    | "nextAction"
    | "nonhuman"
    | "normal"
    | "note"
    | "objective"
    | "open"
    | "path"
    | "policyEnabled"
    | "previous"
    | "priority"
    | "progress"
    | "recentOutcomes"
    | "refresh"
    | "removeSource"
    | "reopen"
    | "replaceUnavailable"
    | "resetResponse"
    | "responsibilities"
    | "responsibility"
    | "resume"
    | "retry"
    | "reviewRequired"
    | "save"
    | "sources"
    | "space"
    | "submitted"
    | "suggestion"
    | "title"
    | "unavailableSource"
    | "unchecked"
    | "unconfirmed"
    | "urgent",
    string
  >;
  sandboxNetwork: {
    title: string;
    blocked: (
      name: string,
      host: string,
      minutes: number,
      provider: string,
    ) => string;
    neverAllowed: string;
    allow: (minutes: number) => string;
    keepBlocked: string;
    allowed: (host: string, minutes: number, name: string) => string;
    kept: (host: string) => string;
    managersOnly: (name: string) => string;
    deniedAsk: (
      name: string,
      host: string,
      supervisor: string,
      support: string,
    ) => string;
    denied: (name: string, host: string, provider: string) => string;
  };
  agentInstances: Record<
    | "description"
    | "legacyUnavailable"
    | "administratorSupport"
    | "bringBack"
    | "makeEmployee"
    | "makeEmployeeNotice"
    | "name"
    | "custody"
    | "personal"
    | "company"
    | "supervisor"
    | "definition"
    | "start"
    | "home"
    | "spaces"
    | "manage"
    | "rename"
    | "suspend"
    | "archive"
    | "delete"
    | "restore"
    | "adopt"
    | "supervise"
    | "grant"
    | "member"
    | "use"
    | "inspect"
    | "manageAccess"
    | "grantsNotice"
    | "retention"
    | "retry"
    | "abandon"
    | "abandoned"
    | "unconfirmed"
    | "refresh"
    | "loading"
    | "none"
    | "more"
    | "memory"
    | "fact"
    | "addFact"
    | "removeFact"
    | "clearMemory"
    | "exportMemory"
    | "importMemory"
    | "replaceMemory"
    | "confirm"
    | "companyCopy"
    | "companyCopyNotice"
    | "status"
    | "active"
    | "suspended"
    | "archived"
    | "deleted"
    | "provisioning"
    | "instructions"
    | "businessDefinition"
    | "useOnly"
    | "operationError"
    | "source",
    string
  > & {
    welcome: {
      title: (name: string) => string;
      body: (name: string) => string;
      ownFiles: (name: string) => string;
      assistantNoMyFiles: (name: string) => string;
      sharedReadable: (name: string) => string;
      prefilled: string;
      firstMessage: (name: string) => string;
      tipFiles: (name: string) => string;
      tipWork: (name: string) => string;
      tipAccess: string;
      companyNotice: (name: string) => string;
    };
    chat: {
      subline: (
        kind: "personal" | "company",
        own: boolean,
        owner: string,
        supervisor: string,
      ) => string;
      alsoWorking: string;
      colleagues: (person: string, name: string) => string;
    };
    fileNeedsInspect: (name: string, supervisor: string) => string;
    busy: (name: string) => string;
    conversationLimit: (name: string) => string;
    restarting: (name: string) => string;
    startingElsewhere: (name: string) => string;
    fullStart: (name: string, support: string) => string;
    fullStartAsk: (name: string, supervisor: string, support: string) => string;
    interruptedByManager: (name: string) => string;
    interruptedByRecovery: (provider: string, name: string) => string;
    interrupted: (name: string) => string;
    interruptedByService: string;
    create: {
      title: (kind: "personal" | "company") => string;
      step: (n: number) => string;
      namePlaceholder: string;
      nameHelp: (kind: "personal" | "company") => string;
      nameInvalid: string;
      roleHeading: (name: string) => string;
      roleHeadingUnnamed: (kind: "personal" | "company") => string;
      roleBlank: string;
      roleBlankDescription: string;
      rolesNone: string;
      rolesFailed: string;
      replaceBrief: string;
      keepBrief: string;
      useRoleBrief: string;
      brief: (name: string) => string;
      briefUnnamed: (kind: "personal" | "company") => string;
      briefHelp: (name: string) => string;
      briefHelpUnnamed: (kind: "personal" | "company") => string;
      briefPlaceholder: string;
      briefRequired: string;
      briefTooLong: string;
      useCustomAgent: string;
      useRole: string;
      customAgent: string;
      customAgentNone: string;
      next: string;
      back: string;
      supervisorHeading: (name: string) => string;
      supervisorHelp: (name: string) => string;
      settingUp: (name: string) => string;
      slow: string;
      unconfirmed: (name: string) => string;
      failed: (name: string) => string;
      briefFailed: string;
      companyRefused: string;
      addedNoConversation: (name: string) => string;
    };
    files: {
      root: (name: string) => string;
      name: string;
      size: string;
      modified: string;
      uploadsHint: (name: string) => string;
      outputsHint: (name: string) => string;
      producedBy: (name: string, date: string) => string;
      add: string;
      drop: (name: string) => string;
      choose: string;
      newFolder: string;
      folderName: string;
      newName: string;
      deleteConfirm: (file: string) => string;
      needsInspect: (name: string, supervisor: string) => string;
      readOnlyHint: (name: string) => string;
      emptyRoot: (name: string) => string;
      emptyRootReader: string;
      uploading: (percent: number) => string;
      saved: (name: string) => string;
      savedAs: (file: string) => string;
      refused: (file: string) => string;
      noAccess: (name: string) => string;
      full: (name: string, support: string) => string;
      fullAsk: (name: string, supervisor: string, support: string) => string;
      tooLarge: (file: string, max: string) => string;
      listFailed: (name: string) => string;
      accessNotSaved: (name: string) => string;
      listLive: (name: string) => string;
      invalidName: string;
      reserved: (name: string) => string;
      nameTaken: string;
      fileChanged: string;
      folderNotEmpty: string;
      notActive: (name: string) => string;
      notFound: string;
      actionFailed: string;
    };
    memoryNotice: (name: string) => string;
    confirmMemory: (name: string) => string;
    memoryUnavailable: (name: string) => string;
    memoryTooLarge: string;
    memoryInvalid: string;
    providerFallback: string;
    filesBusy: (name: string) => string;
    assistants: {
      title: string;
      description: string;
      add: string;
      none: string;
      othersHeading: string;
      ownerLine: (owner: string) => string;
      allowance: (count: number, max: number, name: string) => string;
      limitReached: (count: number, max: number, name: string) => string;
      limitUnfinished: (name: string) => string;
      noneFree: (administrator: boolean, provider: string) => string;
      noPermission: string;
      notOffered: string;
      listFailed: string;
    };
    employees: {
      title: string;
      description: string;
      add: string;
      none: string;
      adminsAdd: string;
      supervisorLine: (supervisor: string) => string;
      allowance: (count: number, max: number) => string;
      limitReached: (max: number, provider: string) => string;
      notOffered: string;
      listFailed: string;
    };
    group: {
      active: string;
      paused: string;
      retired: string;
      unfinished: string;
    };
    unfinished: (name: string) => string;
    finishSetup: string;
    remove: string;
    unfinishedElsewhere: (name: string) => string;
    unfinishedCompleted: (name: string) => string;
    marker: { ai: string; aiAssistant: string; aiEmployee: string };
    conversations: {
      with: (person: string) => string;
      empty: string;
      untitled: string;
      yours: string;
      running: string;
      ownOnly: string;
      loadFailed: string;
      noUse: (supervisor: string) => string;
    };
    access: {
      heading: (name: string) => string;
      use: string;
      inspect: string;
      manage: string;
      useHelp: (name: string) => string;
      inspectHelp: (name: string) => string;
      chatOnlyNote: (person: string, name: string) => string;
      manageHelp: (name: string) => string;
      supervisorNote: (name: string) => string;
      supervisorHint: (name: string) => string;
      filesFollow: (name: string) => string;
      onlyYou: string;
      manageIncludes: string;
      manageNeedsInspect: (name: string) => string;
      lastManager: (name: string) => string;
      ownManage: string;
      add: string;
      searchHint: string;
      noResults: string;
      searchHintFull: string;
      find: string;
      notFoundFull: string;
      searchFailed: string;
      alreadyHasAccess: (person: string) => string;
      saved: string;
      remove: string;
      removeConfirm: (person: string, name: string) => string;
    };
    interrupt: {
      title: (name: string) => string;
      body: (change: string, name: string, list: string) => string;
      bodyUnknown: (change: string, name: string) => string;
      grant: string;
      supervise: string;
      adopt: string;
      reply: (person: string) => string;
      work: (objective: string) => string;
      wait: (name: string) => string;
      now: string;
      pending: (name: string) => string;
    };
    lifecycleAction: {
      suspend: string;
      restore: string;
      archive: string;
      delete: string;
      adopt: string;
      supervise: string;
      grant: string;
    };
    unavailable: StorageUnavailableTranslations;
    header: {
      line: (
        kind: "personal" | "company",
        own: boolean,
        owner: string,
        supervisor: string,
      ) => string;
      yourAccess: string;
    };
    state: {
      working: string;
      idle: string;
      lastActive: (time: string) => string;
      neverUsed: string;
      paused: string;
      retired: string;
      deleted: string;
      unfinished: string;
      unavailable: string;
      changing: string;
    };
    notFound: string;
    backToList: (kind: "personal" | "company") => string;
    pausedNotice: (name: string) => string;
    retiredNotice: (name: string) => string;
    deletedNotice: (name: string) => string;
    accessWithdrawn: (name: string) => string;
    tabs: {
      conversations: string;
      files: string;
      work: string;
      memory: string;
      access: string;
      settings: string;
    };
    pending: (name: string) => string;
    settings: {
      renamed: string;
      assignments: (name: string) => string;
      briefNote: (name: string) => string;
      briefApplied: string;
      useCustomAgent: string;
      pauseConfirm: (name: string) => string;
      runningNow: (name: string) => string;
      retireConfirm: (name: string, kind: "personal" | "company") => string;
      deleteConfirm: (name: string, kind: "personal" | "company") => string;
      deleteUnverified: (name: string, provider: string) => string;
      typeName: (name: string) => string;
      deleting: (name: string) => string;
      deleted: (name: string) => string;
      deletedClearing: (name: string, kind: "personal" | "company") => string;
      abandonConfirm: (name: string) => string;
      abandonRefused: string;
      employeeName: string;
      employeeAdded: (name: string) => string;
      automatic: (name: string) => string;
    };
  };
  storageSpaces: {
    agentFilesOf: (name: string) => string;
    agentFilesRecoveryNote: (name: string) => string;
    agentFilesUnknown: string;
    agentFilesRecoveryUnknown: string;
    membersHint: string;
    createOff: string;
    confirmAction: string;
    recovery: string;
    recoveryNotice: string;
    backup: string;
    archive: string;
    deleteSpace: string;
    restore: string;
    consistentBackup: string;
    attachedEnvironments: string;
    retireEnvironments: string;
    providerUnavailable: string;
    pendingOperation: string;
    acceptCurrentState: string;
    restoreNotice: string;
    deleteNotice: string;
    acceptStateNotice: string;
    confirmLifecycle: string;
    recoveryPending: string;
    title: string;
    description: string;
    unavailable: string;
    unavailableHint: string;
    newSpace: string;
    spaceName: string;
    custody: string;
    personal: string;
    company: string;
    noSpaces: string;
    openRoot: string;
    path: string;
    newFolder: string;
    newFile: string;
    upload: string;
    loadError: string;
    operationError: string;
    truncated: string;
    readOnly: string;
    editorNotice: string;
    confirmRemove: string;
    removeHint: string;
    newName: string;
    textContent: string;
    quota: string;
    copy: string;
    copySource: string;
    sourcePath: string;
    destinationPath: string;
    disclosure: string;
    disclosureLabel: string;
    limitText: string;
    unsupportedEntry: string;
    accessLabel: string;
    utf8Notice: string;
    back: string;
    empty: string;
    reload: string;
    noEditor: string;
  };
  extensions: {
    fileDestinationsUnavailable: string;
    navigation: string;
    pageLoading: string;
    pageUnavailable: string;
    pageUnavailableHint: string;
    reload: string;
    viewFailed: string;
    actionFailed: string;
  };
  // Locale meta
  locale: {
    localName: string;
  };

  // Common
  common: {
    home: string;
    settings: string;
    delete: string;
    edit: string;
    rename: string;
    renameFailed: string;
    share: string;
    openInNewWindow: string;
    close: string;
    more: string;
    search: string;
    loadMore: string;
    download: string;
    thinking: string;
    artifacts: string;
    public: string;
    custom: string;
    notAvailableInDemoMode: string;
    loading: string;
    version: string;
    lastUpdated: string;
    code: string;
    preview: string;
    cancel: string;
    save: string;
    install: string;
    create: string;
    import: string;
    export: string;
    exportAsMarkdown: string;
    exportAsJSON: string;
    exportSuccess: string;
    exportFailed: string;
    regenerate: string;
    editAndRerun: string;
    updateAndRerun: string;
    editRerunWarning: string;
    branch: string;
    showArtifacts: string;
    browser: string;
    showBrowser: string;
  };

  runProgress: {
    residentPreparing: (name: string) => string;
    residentStarting: (name: string) => string;
    residentStartingLong: (name: string) => string;
    residentStartFailed: (name: string, support: string) => string;
    preparing: string;
    workspaceStarting: string;
    thinking: string;
  };

  runDuration: {
    reasoning: string;
    working: string;
    completedIn: (duration: string) => string;
    description: string;
    lessThanSecond: string;
    hours: (value: number) => string;
    minutes: (value: number) => string;
    seconds: (value: number) => string;
    separator: string;
  };

  home: {
    docs: string;
    blog: string;
  };

  // Welcome
  welcome: {
    greeting: string;
    description: string;
    startersLabel: string;
    startersHint: string;
    createYourOwnSkill: string;
    createYourOwnSkillDescription: string;
  };

  // Clipboard
  clipboard: {
    copyToClipboard: string;
    copiedToClipboard: string;
    failedToCopyToClipboard: string;
    linkCopied: string;
  };

  artifactEditing: {
    unsaved: string;
    saving: string;
    saved: string;
    exit: string;
    discard: string;
    discardChanges: string;
    conflict: string;
    conflictShort: string;
    runInProgress: string;
    saveFailed: string;
  };

  artifactPreview: {
    limited: (previewSize: string, totalSize?: string) => string;
    loadFullFile: string;
    loadingFullFile: string;
    previewFailed: string;
    viewSource: string;
    missingTarget: string;
  };

  artifactViews: {
    unsupported: string;
    imageUnavailable: string;
    table: string;
    exports: string;
    exportsHint: string;
    checkingFiles: string;
    filesUncertain: string;
    noAvailableFiles: string;
    retry: string;
    collection: (name: string) => string;
    ignoredCollection: string;
  };

  artifactArchive: {
    downloadCurrent: (count: number) => string;
    currentVersionNotice: string;
    downloadFailed: string;
  };

  // Citations
  citations: {
    sourcesSummary: (count: number) => string;
    citeCount: (count: number) => string;
    copyReference: (title: string) => string;
    copiedReference: (title: string) => string;
  };

  // Workspace Changes
  workspaceChanges: {
    title: string;
    editedTitle: (count: number) => string;
    badge: (count: number, additions: number, deletions: number) => string;
    viewChanges: string;
    created: string;
    modified: string;
    deleted: string;
    openFile: string;
    loading: string;
    noChanges: string;
    diffUnavailable: string;
    binaryUnavailable: string;
    largeUnavailable: string;
    sensitiveUnavailable: string;
    truncatedUnavailable: string;
    symlinkUnavailable: string;
    truncatedSummary: string;
  };

  // Files a run produced but never presented
  artifactDelivery: {
    verificationTitle: string;
    verificationDescription: string;
    title: (count: number) => string;
    description: (count: number) => string;
    shownOfTotal: (shown: number, total: number) => string;
    receiptToast: string;
  };

  // Input Box
  inputBox: {
    agentFile: {
      attach: (name: string) => string;
      hint: (name: string) => string;
      sendWaits: string;
      uploading: (percent: number) => string;
      saved: (name: string) => string;
      savedAs: (file: string) => string;
      refused: (file: string) => string;
      noAccess: (name: string) => string;
      tooLarge: (file: string, max: string) => string;
    };
    placeholder: string;
    disclaimer: string;
    /** The same line in a company's workspace, which is not the product's to sign. */
    assistantDisclaimer: string;
    createSkillPrompt: string;
    addAttachments: string;
    inputPolish: string;
    inputPolishing: string;
    inputPolishNoChanges: string;
    inputPolishFailed: string;
    inputPolishUndo: string;
    inputPolishCancel: string;
    voiceInputStartLabel: string;
    voiceInputStopLabel: string;
    voiceInputStart: string;
    voiceInputStop: string;
    voiceInputListening: string;
    voiceInputUnsupported: string;
    voiceInputPermissionDenied: string;
    voiceInputMicrophoneUnavailable: string;
    voiceInputUnsupportedLanguage: string;
    voiceInputNetworkError: string;
    voiceInputNoSpeech: string;
    voiceInputFailed: string;
    mode: string;
    flashMode: string;
    flashModeDescription: string;
    reasoningMode: string;
    reasoningModeDescription: string;
    proMode: string;
    proModeDescription: string;
    ultraMode: string;
    ultraModeDescription: string;
    reasoningEffort: string;
    reasoningEffortMinimal: string;
    reasoningEffortMinimalDescription: string;
    reasoningEffortLow: string;
    reasoningEffortLowDescription: string;
    reasoningEffortMedium: string;
    reasoningEffortMediumDescription: string;
    reasoningEffortHigh: string;
    reasoningEffortHighDescription: string;
    modelsLoading: string;
    modelsUnavailable: string;
    modelsLoadFailed: string;
    modelsRetry: string;
    searchModels: string;
    surpriseMe: string;
    surpriseMePrompt: string;
    followupLoading: string;
    followupConfirmTitle: string;
    followupConfirmDescription: string;
    followupConfirmAppend: string;
    followupConfirmReplace: string;
    suggestionPlaceholderRequired: string;
    goalCommandDescription: string;
    compactCommandDescription: string;
    goalLabel: string;
    goalContinuing: string;
    goalContinuationTooltip: string;
    goalSet: string;
    goalCleared: string;
    goalNone: string;
    goalActive: string;
    goalFailed: string;
    goalTooLong: string;
    goalLengthCounter: string;
    compactSuccess: string;
    compactSkipped: string;
    compactFailed: string;
    suggestions: {
      suggestion: string;
      prompt: string;
      icon: LucideIcon;
    }[];
    suggestionsCreate: (
      | {
          suggestion: string;
          prompt: string;
          icon: LucideIcon;
        }
      | {
          type: "separator";
        }
    )[];
    pleaseWaitStreaming: string;
  };

  // Sidebar
  sidebar: {
    myAssistants: string;
    aiEmployees: string;
    assistantsOff: string;
    aiEmployeesOff: string;
    agentsUnavailable: string;
    recentChats: string;
    newChat: string;
    chats: string;
    demoChats: string;
    agents: string;
    scheduledTasks: string;
    agentsDisabledTooltip: string;
    channels: string;
    files: string;
  };

  // My files: what the person kept, across conversations
  files: {
    saveFull: (support: string) => string;
    busy: string;
    title: string;
    filterLabel: string;
    filterPlaceholder: string;
    clearFilter: string;
    sortLabel: string;
    sortName: string;
    sortNewest: string;
    foundCount: (found: number, loaded: number) => string;
    noMatches: string;
    noMatchesHint: string;
    description: string;
    empty: string;
    emptyHint: string;
    loadFailed: string;
    retry: string;
    name: string;
    folder: string;
    size: string;
    modified: string;
    actions: string;
    download: string;
    delete: string;
    deleteTitle: string;
    deleteConfirm: (name: string) => string;
    deleting: string;
    deleted: (name: string) => string;
    deleteFailed: string;
    truncated: (count: number) => string;
    saveToMyFiles: string;
    saving: string;
    saved: (name: string) => string;
    savedToFolder: (name: string, folder: string) => string;
    savedMany: (count: number) => string;
    savedManyToFolder: (count: number, folder: string) => string;
    savedSome: (kept: number, total: number) => string;
    saveFailed: string;
    openMyFiles: string;
    /** The page holding both tabs. */
    pageTitle: string;
  };

  // The company's Shared area: what anyone published, readable by everyone.
  shared: {
    full: (support: string) => string;
    busy: string;
    unchecked: string;
    title: string;
    description: string;
    empty: string;
    emptyHint: string;
    loadFailed: string;
    publishedBy: (who: string) => string;
    publishedByNobody: string;
    publisher: string;
    published: string;
    remove: string;
    removeTitle: string;
    removeConfirm: (name: string) => string;
    removing: string;
    removed: (name: string) => string;
    removeFailed: string;
    truncated: (count: number) => string;
    shareWithEveryone: string;
    sharing: string;
    shared: (name: string) => string;
    sharedInFolder: (name: string, folder: string) => string;
    sharedMany: (count: number) => string;
    sharedManyInFolder: (count: number, folder: string) => string;
    sharedSome: (published: number, total: number) => string;
    shareFailed: string;
    openShared: string;
    undo: string;
    undone: (name: string) => string;
    alreadyShared: (name: string) => string;
    alreadySharedAs: (name: string, sharedAs: string) => string;
    alreadySharedMany: (count: number) => string;
  };

  // Thread-scoped MCP background tasks
  backgroundTasks: {
    label: string;
    title: string;
    description: string;
    active: string;
    recent: string;
    empty: string;
    emptyHint: string;
    loadFailed: string;
    retry: string;
    cancel: string;
    cancelling: string;
    cancelFailed: string;
    cancellationRetrying: (attempt: number) => string;
    notificationRetrying: (attempt: number) => string;
    notificationStopped: string;
    trackingDegraded: string;
    viewDetails: string;
    hideDetails: string;
    detailsFailed: string;
    result: string;
    resultArtifact: string;
    inputRequired: string;
    inputUnavailable: string;
    lastPollError: string;
    created: (time: string) => string;
    updated: (time: string) => string;
    status: {
      submitted: string;
      working: string;
      inputRequired: string;
      completed: string;
      failed: string;
      cancelled: string;
    };
  };

  subagentBatches: {
    label: string;
    title: string;
    description: string;
    workerUnavailable: string;
    empty: string;
    emptyHint: string;
    loadFailed: string;
    active: string;
    recent: string;
    pause: string;
    resume: string;
    cancel: string;
    retryItem: string;
    exportResults: string;
    viewItems: string;
    hideItems: string;
    itemsFailed: string;
    progress: (completed: number, total: number) => string;
    limits: (live: number, running: number) => string;
    status: {
      queued: string;
      running: string;
      paused: string;
      completed: string;
      failed: string;
      cancelled: string;
    };
  };

  // Scheduled tasks
  scheduledTasks: {
    scheduleType: { cron: string; once: string };
    preset: {
      label: string;
      hourly: string;
      daily: string;
      weekly: string;
      monthly: string;
      custom: string;
    };
    fields: {
      minute: string;
      time: string;
      weekday: string;
      dayOfMonth: string;
      cron: string;
      cronPlaceholder: string;
      runAt: string;
      timezone: string;
    };
    weekdays: {
      mon: string;
      tue: string;
      wed: string;
      thu: string;
      fri: string;
      sat: string;
      sun: string;
    };
    preview: string;
    cronHelp: string;
    create: {
      title: string;
      taskTitle: string;
      prompt: string;
      submit: string;
      fillRequired: string;
    };
    context: {
      fresh: string;
      reuse: string;
      threadIdPlaceholder: string;
      reuseNoticeTitle: string;
      reuseNoticeDescription: string;
    };
    scheduler: {
      offTitle: string;
      offDescription: string;
      unavailableTitle: string;
      unavailableDescription: string;
      stoppedTitle: string;
      stoppedDescription: string;
      overdue: string;
    };
    filters: {
      allStatuses: string;
      enabled: string;
      paused: string;
      completed: string;
      failed: string;
      allTypes: string;
      cron: string;
      once: string;
    };
    detail: {
      contextMode: string;
      thread: string;
      lastThread: string;
      schedule: string;
      nextRun: string;
      lastRun: string;
      lastRunId: string;
      lastError: string;
      runsCount: string;
      runsCountOne: string;
      noRuns: string;
      noSelection: string;
      filteredByThread: string;
      loadFailed: string;
    };
    actions: {
      edit: string;
      cancelEdit: string;
      pause: string;
      resume: string;
      trigger: string;
      duplicate: string;
      duplicateTitleSuffix: string;
      delete: string;
    };
    deleteConfirm: string;
    errors: {
      create: string;
      update: string;
      pause: string;
      resume: string;
      trigger: string;
      delete: string;
    };
    edit: {
      titlePlaceholder: string;
      promptPlaceholder: string;
      submit: string;
    };
    status: {
      enabled: string;
      paused: string;
      running: string;
      completed: string;
      failed: string;
      cancelled: string;
    };
    runTrigger: { scheduled: string; manual: string };
    runStatus: {
      queued: string;
      launching: string;
      running: string;
      success: string;
      failed: string;
      skipped: string;
      interrupted: string;
    };
    recipes: {
      label: string;
      trending: { title: string; desc: string };
      news: { title: string; desc: string };
      issues: { title: string; desc: string };
      weekly: { title: string; desc: string };
    };
  };

  // Agents
  agents: {
    title: string;
    description: string;
    newAgent: string;
    emptyTitle: string;
    emptyDescription: string;
    featureDisabledTitle: string;
    featureDisabledDescription: string;
    chat: string;
    delete: string;
    deleteConfirm: string;
    deleteSuccess: string;
    newChat: string;
    createPageTitle: string;
    createPageSubtitle: string;
    nameStepTitle: string;
    nameStepHint: string;
    nameStepPlaceholder: string;
    nameStepContinue: string;
    nameStepInvalidError: string;
    nameStepAlreadyExistsError: string;
    nameStepNetworkError: string;
    nameStepCheckError: string;
    nameStepCheckErrorWithDetail: string;
    nameStepApiDisabledError: string;
    nameStepBootstrapMessage: string;
    save: string;
    saving: string;
    saveRequested: string;
    saveHint: string;
    saveCommandMessage: string;
    agentCreatedPendingRefresh: string;
    more: string;
    agentCreated: string;
    startChatting: string;
    backToGallery: string;
    settings: string;
    settingsTitle: string;
    settingsDescription: string;
    settingsModel: string;
    settingsModelDefault: string;
    settingsTemperature: string;
    settingsTemperatureHint: string;
    settingsMaxTokens: string;
    settingsMaxTokensPlaceholder: string;
    settingsThinking: string;
    settingsThinkingOn: string;
    settingsThinkingOff: string;
    settingsReasoningEffort: string;
    settingsInherit: string;
    settingsSaved: string;
    settingsInvalidTemperature: string;
    settingsInvalidMaxTokens: string;
  };

  // Breadcrumb
  breadcrumb: {
    workspace: string;
    chats: string;
  };

  // Workspace
  workspace: {
    settingsAndMore: string;
    contactSupport: string;
    providerSupport: (providerName: string) => string;
    about: string;
    /** About, for a workspace that carries a company's name. */
    aboutCompany: (companyName: string) => string;
    logout: string;
    gatewayUnavailable: string;
    gatewayUnavailableRetrying: string;
    modelLoadFailed: string;
    modelLoadRetry: string;
    modelLoadRetrying: string;
  };

  // Download all my data
  accountExport: {
    menuItem: string;
    title: string;
    description: string;
    onlyYours: string;
    closeAnytime: string;
    start: string;
    preparing: string;
    conversationsProgress: (done: number, total: number) => string;
    filesProgress: (done: number, total: number) => string;
    stop: string;
    ready: string;
    includesUpTo: (time: string) => string;
    download: string;
    downloadPart: (number: number, count: number) => string;
    downloaded: string;
    multiPart: string;
    availableUntil: (time: string) => string;
    keptWhileDownloading: string;
    allDownloaded: string;
    skipped: (count: number) => string;
    deleteNow: string;
    deleteFailed: string;
    gone: string;
    noSpace: string;
    failed: string;
    tryAgain: string;
    busy: string;
    unavailable: string;
    loadFailed: string;
  };

  // Conversation
  conversation: {
    noMessages: string;
    startConversation: string;
    branchCreated: string;
    branchFailed: string;
    streamReplayGap: string;
    outlineLabel: string;
    outlineAttachmentFallback: string;
  };

  // Chats
  chats: {
    deleteChat: string;
    deleteConfirm: (title: string) => string;
    deleteFailed: string;
    deleting: string;
    searchChats: string;
    branchLabel: (title: string, parentTitle: string) => string;
    loadMoreToSearch: string;
    loadingMore: string;
    loadOlderChats: string;
    pinChat: string;
    unpinChat: string;
    pinChatFailed: string;
  };

  // Sidecar
  sidecar: {
    title: string;
    open: string;
    close: string;
    delete: string;
    deleteConfirm: string;
    deleteSuccess: string;
    deleteFailed: string;
    addToConversation: string;
    askInSideChat: string;
    reference: string;
    selectedTextFragment: string;
    selectedTextFragments: string;
    clearReferences: string;
    emptyTitle: string;
    emptyDescription: string;
    placeholder: string;
    send: string;
    sendFailed: string;
    noContext: string;
    continuing: string;
    selectionCrossesMessages: string;
  };

  // Channels
  channels: {
    title: string;
    connect: string;
    modify: string;
    reconnect: string;
    disconnect: string;
    connected: string;
    notConnected: string;
    pending: string;
    revoked: string;
    disabled: string;
    unconfigured: string;
    unavailable: string;
    unavailableShort: string;
    setupTitle: (name: string) => string;
    setupEditTitle: (name: string) => string;
    setupDescription: string;
    saveAndConnect: string;
    saveChanges: string;
    descriptions: Record<string, string>;
    connectedAs: (name: string) => string;
  };

  // Page titles (document title)
  pages: {
    appName: string;
    chats: string;
    newChat: string;
    untitled: string;
  };

  // Tool calls
  toolCalls: {
    moreSteps: (count: number) => string;
    lessSteps: string;
    executeCommand: string;
    presentFiles: string;
    needYourHelp: string;
    useTool: (toolName: string) => string;
    searchForRelatedInfo: string;
    searchForRelatedImages: string;
    searchFor: (query: string) => string;
    searchForRelatedImagesFor: (query: string) => string;
    searchOnWebFor: (query: string) => string;
    viewWebPage: string;
    listFolder: string;
    readFile: string;
    writeFile: string;
    clickToViewContent: string;
    writeTodos: string;
    skillInstallTooltip: string;
    browserNavigate: (url: string) => string;
    browserNavigateGeneric: string;
    browserClick: string;
    browserType: string;
    browserSnapshot: string;
    browserGetText: string;
    browserBack: string;
    browserScreenshot: string;
    browserClose: string;
  };

  humanInput: {
    answered: string;
    pending: string;
    readOnly: string;
    otherLabel: string;
    otherPlaceholder: string;
    submit: string;
    emptyError: string;
    requiredError: string;
    requiredA11yLabel: string;
    selectPlaceholder: string;
    answeredValue: (value: string) => string;
  };

  // Uploads
  uploads: {
    filesFull: (file: string, support: string) => string;
    uploading: string;
    uploadingFiles: string;
    limitsHint: (
      maxFiles: number,
      maxFileSize: string,
      maxTotalSize: string,
    ) => string;
    filesTooLarge: (files: string, maxFileSize: string) => string;
    tooManyFiles: (count: number, maxFiles: number) => string;
    totalSizeTooLarge: (count: number, maxTotalSize: string) => string;
  };

  // Subtasks
  subtasks: {
    subtask: string;
    executing: (count: number) => string;
    in_progress: string;
    completed: string;
    failed: string;
  };

  // Token Usage
  tokenUsage: {
    title: string;
    label: string;
    input: string;
    output: string;
    total: string;
    view: string;
    unavailable: string;
    unavailableShort: string;
    collecting: string;
    note: string;
    presets: {
      off: string;
      summary: string;
      perTurn: string;
      debug: string;
    };
    presetDescriptions: {
      off: string;
      summary: string;
      perTurn: string;
      debug: string;
    };
    finalAnswer: string;
    stepTotal: string;
    sharedAttribution: string;
    subagent: (description: string) => string;
    startTodo: (content: string) => string;
    completeTodo: (content: string) => string;
    updateTodo: (content: string) => string;
    removeTodo: (content: string) => string;
  };

  contextUsage: {
    label: string;
    title: string;
    badgeAriaLabel: (percentage: string) => string;
  };

  // Shortcuts
  shortcuts: {
    searchActions: string;
    noResults: string;
    actions: string;
    keyboardShortcuts: string;
    keyboardShortcutsDescription: string;
    openCommandPalette: string;
    toggleSidebar: string;
  };

  // Settings
  settings: {
    providerEnablement: string;
    title: string;
    description: string;
    /** The same line in a company's workspace, which is not the product's to describe. */
    descriptionForCompany: string;
    sections: {
      account: string;
      appearance: string;
      channels: string;
      integrations: string;
      memory: string;
      tools: string;
      subagents: string;
      skills: string;
      notification: string;
      about: string;
    };
    memory: {
      title: string;
      description: string;
      empty: string;
      rawJson: string;
      exportButton: string;
      exportSuccess: string;
      importButton: string;
      importConfirmTitle: string;
      importConfirmDescription: string;
      importFileLabel: string;
      importInvalidFile: string;
      importSuccess: string;
      manualFactSource: string;
      addFact: string;
      addFactTitle: string;
      editFactTitle: string;
      addFactSuccess: string;
      editFactSuccess: string;
      clearAll: string;
      clearAllConfirmTitle: string;
      clearAllConfirmDescription: string;
      clearAllSuccess: string;
      factDeleteConfirmTitle: string;
      factDeleteConfirmDescription: string;
      factDeleteSuccess: string;
      factContentLabel: string;
      factCategoryLabel: string;
      factConfidenceLabel: string;
      factContentPlaceholder: string;
      factCategoryPlaceholder: string;
      factConfidenceHint: string;
      factSave: string;
      factValidationContent: string;
      factValidationConfidence: string;
      noFacts: string;
      summaryReadOnly: string;
      memoryFullyEmpty: string;
      factPreviewLabel: string;
      searchPlaceholder: string;
      filterAll: string;
      filterFacts: string;
      filterSummaries: string;
      noMatches: string;
      markdown: {
        overview: string;
        userContext: string;
        work: string;
        personal: string;
        topOfMind: string;
        historyBackground: string;
        recentMonths: string;
        earlierContext: string;
        longTermBackground: string;
        updatedAt: string;
        facts: string;
        empty: string;
        table: {
          category: string;
          confidence: string;
          confidenceLevel: {
            veryHigh: string;
            high: string;
            normal: string;
            unknown: string;
          };
          content: string;
          source: string;
          createdAt: string;
          view: string;
        };
      };
    };
    appearance: {
      themeTitle: string;
      themeDescription: string;
      system: string;
      light: string;
      dark: string;
      systemDescription: string;
      lightDescription: string;
      darkDescription: string;
      languageTitle: string;
      languageDescription: string;
    };
    tools: {
      title: string;
      description: string;
      adminRequired: string;
      empty: string;
      addServer: string;
      addServerDescription: string;
      addServerPlaceholder: string;
      serverDefinitionLabel: string;
      definitionEmpty: string;
      definitionInvalidJson: string;
      definitionRootNotObject: string;
      definitionNoServers: string;
      definitionServerNotObject: string;
      editServer: string;
      editServerDescription: string;
      editSingleServer: string;
      editServerNameMismatch: string;
      serverAlreadyExists: string;
      removeServer: string;
      removeServerDescription: string;
      unnamedServer: string;
    };
    subagents: {
      title: string;
      description: string;
      executionNote: string;
      adminNote: string;
      create: string;
      empty: string;
      sourceBuiltin: string;
      sourceConfig: string;
      sourceManaged: string;
      conflict: string;
      overridden: string;
      createTitle: string;
      editTitle: string;
      name: string;
      nameHint: string;
      displayName: string;
      descriptionLabel: string;
      systemPrompt: string;
      model: string;
      inheritModel: string;
      tools: string;
      skills: string;
      listModeAll: string;
      listModeNone: string;
      listModeSelected: string;
      listNamesPlaceholder: string;
      maxTurns: string;
      timeout: string;
      created: string;
      saved: string;
      deleted: string;
      deleteConfirm: string;
      bindingTitle: string;
      bindingDescription: string;
      allAllowed: string;
      noneAllowed: string;
      selectedAllowed: string;
      missing: string;
    };
    channels: {
      title: string;
      description: string;
      disabled: string;
    };
    integrations: {
      title: string;
      description: string;
      refresh: string;
      install: string;
      reinstall: string;
      installing: string;
      ready: string;
      pending: string;
      available: string;
      unavailable: string;
      connected: string;
      loadFailed: string;
      adminRequired: string;
      lark: {
        title: string;
        description: string;
        skillPack: string;
        gatewayCli: string;
        auth: string;
        sandboxRuntime: string;
        sandboxRuntimeInitContainer: string;
        sandboxRuntimeBroker: string;
        sandboxRuntimeGatewayDownload: string;
        sandboxRuntimeNotReady: string;
        notInstalled: string;
        skillsInstalled: (installed: number, expected: number) => string;
        installedVersion: (version: string) => string;
        updateAvailable: (version: string) => string;
        runtimeVersionMismatch: string;
        authNotConfigured: string;
        authConfigured: string;
        authConfiguredFor: (user: string) => string;
        connect: string;
        authStarting: string;
        checkingConnection: string;
        connectedAction: string;
        requestPermissions: string;
        alreadyConnected: string;
        changeAppButton: string;
        changeAppTitle: string;
        changeAppDescription: string;
        changeAppIdLabel: string;
        changeAppSecretLabel: string;
        changeAppAuthResetNote: string;
        changeAppSubmit: string;
        changeAppReRegister: string;
        changeAppSwitched: string;
        brandFeishu: string;
        brandLark: string;
        connectionStarted: string;
        connectionReady: string;
        authStarted: string;
        authorizationStillPending: string;
        permissionTitle: string;
        permissionDescription: string;
        authDomains: Record<
          | "approval"
          | "apps"
          | "attendance"
          | "base"
          | "calendar"
          | "contact"
          | "docs"
          | "drive"
          | "event"
          | "im"
          | "mail"
          | "markdown"
          | "mindnotes"
          | "minutes"
          | "note"
          | "okr"
          | "sheets"
          | "slides"
          | "task"
          | "vc"
          | "wiki"
          | "all",
          { label: string; description: string }
        >;
        customScopeLabel: string;
        customScopePlaceholder: string;
        customScopeDescription: string;
        openConnectionLinkTitle: string;
        openConnectionLinkDescription: string;
        openAuthLinkTitle: string;
        openAuthLinkDescription: string;
        waitingAuthTitle: string;
        waitingAuthDescription: string;
        openAuthLink: string;
        copyAuthLink: string;
        completeAuth: string;
        continueAuth: string;
        preparingAuthorization: string;
        completingAuth: string;
        authExpiresIn: (seconds: number) => string;
        installingTitle: string;
        installingDescription: string;
        installNextTitle: string;
        installNextDescription: string;
        cliNextTitle: string;
        cliNextDescription: string;
        configuredTitle: string;
        configuredDescription: string;
        connectedTitle: string;
        connectedDescription: string;
        authNextTitle: string;
        authNextDescription: string;
      };
    };
    skills: {
      provided: string;
      privateSkills: string;
      cloneProvided: string;
      cloneTitle: string;
      cloneDescription: string;
      sourceLabel: string;
      nameLabel: string;
      overrideLabel: string;
      overrideDescription: string;
      copyAction: string;
      copyPending: string;
      sourcePlaceholder: string;
      previewFiles: string;
      privateOrigin: string;
      baselineOverride: string;
      copySuccess: string;
      archiveOverride: string;
      title: string;
      description: string;
      createSkill: string;
      emptyTitle: string;
      emptyDescription: string;
      emptyButton: string;
      adminRequired: string;
      installAdminRequired: string;
      installFromFile: string;
      installingArchive: string;
      invalidArchive: string;
      archiveTooLarge: string;
      installFailed: string;
    };
    notification: {
      title: string;
      description: string;
      requestPermission: string;
      deniedHint: string;
      testButton: string;
      testTitle: string;
      testBody: string;
      notSupported: string;
      disableNotification: string;
    };
    account: {
      seats: (count: number, max: number) => string;
      noSeats: (max: number | null, provider: string) => string;
      noSeatsSignOn: (max: number, provider: string) => string;
      profileTitle: string;
      email: string;
      role: string;
      changePasswordTitle: string;
      changePasswordDescription: string;
      ssoProvider: string;
      ssoPasswordDescription: string;
      ssoPasswordMessage: string;
      currentPassword: string;
      newPassword: string;
      confirmNewPassword: string;
      passwordMismatch: string;
      passwordTooShort: string;
      passwordChangedSuccess: string;
      networkError: string;
      updating: string;
      updatePassword: string;
      signOut: string;
      addPersonTitle: string;
      addPersonDescription: string;
      addPersonEmail: string;
      addPersonSubmit: string;
      addPersonAdding: string;
      addPersonAdded: string;
      addPersonOneTimePassword: string;
      addPersonHandOver: string;
      addPersonCopy: string;
      addPersonCopied: string;
      addPersonAnother: string;
    };
    providerKeys: {
      title: string;
      description: string;
      loading: string;
      loadError: string;
      retry: string;
      groupModels: string;
      groupTools: string;
      sourceProduct: string;
      sourceEnvironment: string;
      sourceNone: string;
      unreadable: string;
      unreadableHint: string;
      unreadableHintHostSetting: string;
      refusalNoWrappingKey: string;
      refusalWrappingKeyInvalid: string;
      refusalOperatorModelFile: string;
      renderRefused: string;
      removeConfirm: string;
      changed: string;
      set: string;
      replace: string;
      remove: string;
      save: string;
      saving: string;
      cancel: string;
      keyLabel: string;
      keyPlaceholder: string;
      test: string;
      testing: string;
      testHint: string;
      testAccepted: string;
      testRejected: string;
      testInconclusive: string;
      testTimeout: string;
      testRateLimited: string;
      testNotTestable: string;
      testError: string;
      saved: string;
      removedToEnvironment: string;
      removedToNone: string;
      recentChanges: string;
      eventAdded: string;
      eventReplaced: string;
      eventRemoved: string;
    };
    acknowledge: {
      emptyTitle: string;
      emptyDescription: string;
    };
  };

  // Login / Auth
  login: {
    signInTitle: string;
    createAccountTitle: string;
    email: string;
    emailPlaceholder: string;
    password: string;
    passwordPlaceholder: string;
    rememberMe: string;
    rememberMeDescription: string;
    pleaseWait: string;
    signIn: string;
    createAccount: string;
    createAdminAccount: string;
    adminSetupRequiredTitle: string;
    adminSetupRequiredDescription: string;
    orContinueWith: string;
    ssoHint: string;
    signOnOnlyDescription: string;
    signOnOnlyNoProvider: string;
    continueWith: (provider: string) => string;
    noAccountSignUp: string;
    haveAccountSignIn: string;
    backToHome: string;
    networkError: string;
    serviceUnavailableTitle: string;
    serviceUnavailableDescription: string;
    retry: string;
    authFailed: string;
    errors: {
      sso_failed: string;
      sso_cancelled: string;
      sso_account_exists: string;
      sso_email_unusable: string;
      sso_email_taken: string;
      sso_not_allowed: string;
      sso_no_access: string;
      sso_access_off: string;
    };
    // The same failures where local passwords are no way in: nothing may
    // suggest an email login or a password.
    signOnOnlyErrors: {
      sso_failed: string;
      sso_account_exists: string;
      sso_email_unusable: string;
      sso_email_taken: string;
      sso_no_access: string;
      sso_access_off: string;
    };
  };
}
